/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

import java.io.{File, IOException}
import java.lang.{Long => JLong}
import java.nio.file.{Files, NoSuchFileException}
import java.text.NumberFormat
import java.util.Map.{Entry => JEntry}
import java.util.Optional
import java.util.concurrent.atomic._
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap, TimeUnit}
import java.util.regex.Pattern

import kafka.api.{ApiVersion, KAFKA_0_10_0_IV0}
import kafka.common.{LogSegmentOffsetOverflowException, LongRef, OffsetsOutOfOrderException, UnexpectedAppendOffsetException}
import kafka.message.{BrokerCompressionCodec, CompressionCodec, NoCompressionCodec}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.checkpoints.LeaderEpochCheckpointFile
import kafka.server.epoch.LeaderEpochFileCache
import kafka.server.{BrokerTopicStats, FetchDataInfo, FetchHighWatermark, FetchIsolation, FetchLogEnd, FetchTxnCommitted, LogDirFailureChannel, LogOffsetMetadata, OffsetAndEpoch}
import kafka.utils._
import org.apache.kafka.common.errors._
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import org.apache.kafka.common.requests.ProduceResponse.RecordError
import org.apache.kafka.common.requests.{EpochEndOffset, ListOffsetRequest}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{InvalidRecordException, KafkaException, TopicPartition}

import scala.jdk.CollectionConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.{Seq, Set, mutable}

object LogAppendInfo {
  val UnknownLogAppendInfo = LogAppendInfo(None, -1, RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, -1L,
    RecordConversionStats.EMPTY, NoCompressionCodec, NoCompressionCodec, -1, -1, offsetsMonotonic = false, -1L)

  def unknownLogAppendInfoWithLogStartOffset(logStartOffset: Long): LogAppendInfo =
    LogAppendInfo(None, -1, RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, logStartOffset,
      RecordConversionStats.EMPTY, NoCompressionCodec, NoCompressionCodec, -1, -1,
      offsetsMonotonic = false, -1L)

  /**
   * In ProduceResponse V8+, we add two new fields record_errors and error_message (see KIP-467).
   * For any record failures with InvalidTimestamp or InvalidRecordException, we construct a LogAppendInfo object like the one
   * in unknownLogAppendInfoWithLogStartOffset, but with additiona fields recordErrors and errorMessage
   */
  def unknownLogAppendInfoWithAdditionalInfo(logStartOffset: Long, recordErrors: Seq[RecordError], errorMessage: String): LogAppendInfo =
    LogAppendInfo(None, -1, RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, logStartOffset,
      RecordConversionStats.EMPTY, NoCompressionCodec, NoCompressionCodec, -1, -1,
      offsetsMonotonic = false, -1L, recordErrors, errorMessage)
}

sealed trait LeaderHwChange
object LeaderHwChange {
  case object Increased extends LeaderHwChange
  case object Same extends LeaderHwChange
  case object None extends LeaderHwChange
}

/**
 * Struct to hold various quantities we compute about each message set before appending to the log
 *
 * 保存了 一批待写入的消息的 元数据信息
 *
 * @param firstOffset The first offset in the message set unless the message format is less than V2 and we are appending
 *                    to the follower.
 * @param lastOffset The last offset in the message set
 * @param maxTimestamp The maximum timestamp of the message set.
 * @param offsetOfMaxTimestamp The offset of the message with the maximum timestamp.
 * @param logAppendTime The log append time (if used) of the message set, otherwise Message.NoTimestamp
 * @param logStartOffset The start offset of the log at the time of this append.
 * @param recordConversionStats Statistics collected during record processing, `null` if `assignOffsets` is `false`
 * @param sourceCodec The source codec used in the message set (send by the producer)
 * @param targetCodec The target codec of the message set(after applying the broker compression configuration if any)
 * @param shallowCount The number of shallow messages
 * @param validBytes The number of valid bytes
 * @param offsetsMonotonic Are the offsets in this message set monotonically increasing
 * @param lastOffsetOfFirstBatch The last offset of the first batch
 * @param leaderHwChange Incremental if the high watermark needs to be increased after appending record.
 *                       Same if high watermark is not changed. None is the default value and it means append failed
 *
 */
case class LogAppendInfo(var firstOffset: Option[Long],
                         var lastOffset: Long,
                         var maxTimestamp: Long,
                         var offsetOfMaxTimestamp: Long,
                         var logAppendTime: Long,
                         var logStartOffset: Long,
                         var recordConversionStats: RecordConversionStats,
                         sourceCodec: CompressionCodec,
                         targetCodec: CompressionCodec,
                         shallowCount: Int,
                         validBytes: Int,
                         offsetsMonotonic: Boolean,
                         lastOffsetOfFirstBatch: Long,
                         recordErrors: Seq[RecordError] = List(),
                         errorMessage: String = null,
                         leaderHwChange: LeaderHwChange = LeaderHwChange.None) {
  /**
   * Get the first offset if it exists, else get the last offset of the first batch
   * For magic versions 2 and newer, this method will return first offset. For magic versions
   * older than 2, we use the last offset of the first batch as an approximation of the first
   * offset to avoid decompressing the data.
   */
  def firstOrLastOffsetOfFirstBatch: Long = firstOffset.getOrElse(lastOffsetOfFirstBatch)

  /**
   * Get the (maximum) number of messages described by LogAppendInfo
   * @return Maximum possible number of messages described by LogAppendInfo
   */
  def numMessages: Long = {
    firstOffset match {
      case Some(firstOffsetVal) if (firstOffsetVal >= 0 && lastOffset >= 0) => (lastOffset - firstOffsetVal + 1)
      case _ => 0
    }
  }
}

/**
 * Container class which represents a snapshot of the significant offsets for a partition. This allows fetching
 * of these offsets atomically without the possibility of a leader change affecting their consistency relative
 * to each other. See [[Log.fetchOffsetSnapshot()]].
 */
case class LogOffsetSnapshot(logStartOffset: Long,
                             logEndOffset: LogOffsetMetadata,
                             highWatermark: LogOffsetMetadata,
                             lastStableOffset: LogOffsetMetadata)

/**
 * Another container which is used for lower level reads using  [[kafka.cluster.Partition.readRecords()]].
 */
case class LogReadInfo(fetchedData: FetchDataInfo,
                       divergingEpoch: Option[FetchResponseData.EpochEndOffset],
                       highWatermark: Long,
                       logStartOffset: Long,
                       logEndOffset: Long,
                       lastStableOffset: Long)

/**
 * A class used to hold useful metadata about a completed transaction. This is used to build
 * the transaction index after appending to the log.
 *
 * @param producerId The ID of the producer
 * @param firstOffset The first offset (inclusive) of the transaction
 * @param lastOffset The last offset (inclusive) of the transaction. This is always the offset of the
 *                   COMMIT/ABORT control record which indicates the transaction's completion.
 * @param isAborted Whether or not the transaction was aborted
 */
case class CompletedTxn(producerId: Long, firstOffset: Long, lastOffset: Long, isAborted: Boolean) {
  override def toString: String = {
    "CompletedTxn(" +
      s"producerId=$producerId, " +
      s"firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset, " +
      s"isAborted=$isAborted)"
  }
}

/**
 * A class used to hold params required to decide to rotate a log segment or not.
 */
case class RollParams(maxSegmentMs: Long,
                      maxSegmentBytes: Int,
                      maxTimestampInMessages: Long,
                      maxOffsetInMessages: Long,
                      messagesSize: Int,
                      now: Long)

object RollParams {
  def apply(config: LogConfig, appendInfo: LogAppendInfo, messagesSize: Int, now: Long): RollParams = {
   new RollParams(config.maxSegmentMs,
     config.segmentSize,
     appendInfo.maxTimestamp,
     appendInfo.lastOffset,
     messagesSize, now)
  }
}

sealed trait LogStartOffsetIncrementReason
case object ClientRecordDeletion extends LogStartOffsetIncrementReason {
  override def toString: String = "client delete records request"
}
case object LeaderOffsetIncremented extends LogStartOffsetIncrementReason {
  override def toString: String = "leader offset increment"
}
case object SegmentDeletion extends LogStartOffsetIncrementReason {
  override def toString: String = "segment deletion"
}

/**
 * An append-only log for storing messages.
 *
 * The log is a sequence of LogSegments, each with a base offset denoting the first message in the segment.
 *
 * log 由多个 LogSegments 构成；每个 segment 都有一个 base offset 参数，代表 segment 中第一条消息
 *
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 *
 * 系统会根据配置文件中预设的策略(比如当前 segment 大小、固定的时间间隔等)，创建新的 segment
 *
 * @param _dir The directory in which log segments are created.
 *
 *             这个日志所在的文件夹路径
 *
 * @param config The log configuration settings
 * @param logStartOffset The earliest offset allowed to be exposed to kafka client.
 *                       The logStartOffset can be updated by :
 *                       - user's DeleteRecordsRequest
 *                       - broker's log retention
 *                       - broker's log truncation
 *                       The logStartOffset is used to decide the following:
 *                       - Log deletion. LogSegment whose nextOffset <= log's logStartOffset can be deleted.
 *                         It may trigger log rolling if the active segment is deleted.
 *                       - Earliest offset of the log in response to ListOffsetRequest. To avoid OffsetOutOfRange exception after user seeks to earliest offset,
 *                         we make sure that logStartOffset <= log's highWatermark
 *                       Other activities such as log cleaning are not affected by logStartOffset.
 *
 *                       日志中第一条日志的 offset，这是一个 volatile 类型的变量，存在被多个线程更新的可能。可能得来源有：
 *                       1. 用户删除消息
 *                       2. broker
 *
 * @param recoveryPoint The offset at which to begin recovery--i.e. the first offset which has not been flushed to disk
 * @param scheduler The thread pool scheduler used for background actions
 * @param brokerTopicStats Container for Broker Topic Yammer Metrics
 * @param time The time instance used for checking the clock
 * @param maxProducerIdExpirationMs The maximum amount of time to wait before a producer id is considered expired
 * @param producerIdExpirationCheckIntervalMs How often to check for producer ids which need to be expired
 */
@threadsafe
class Log(@volatile private var _dir: File,
          @volatile var config: LogConfig,
          @volatile var logStartOffset: Long,
          @volatile var recoveryPoint: Long,
          scheduler: Scheduler,
          brokerTopicStats: BrokerTopicStats,
          val time: Time,
          val maxProducerIdExpirationMs: Int,
          val producerIdExpirationCheckIntervalMs: Int,
          val topicPartition: TopicPartition,
          val producerStateManager: ProducerStateManager,
          logDirFailureChannel: LogDirFailureChannel) extends Logging with KafkaMetricsGroup {

  import kafka.log.Log._

  this.logIdent = s"[Log partition=$topicPartition, dir=${dir.getParent}] "

  /* A lock that guards all modifications to the log */
  private val lock = new Object

  // The memory mapped buffer for index files of this log will be closed with either delete() or closeHandlers()
  // After memory mapped buffer is closed, no disk IO operation should be performed for this log
  @volatile private var isMemoryMappedBufferClosed = false

  // Cache value of parent directory to avoid allocations in hot paths like ReplicaManager.checkpointHighWatermarks
  @volatile private var _parentDir: String = dir.getParent

  /* last time it was flushed */
  private val lastFlushedTime = new AtomicLong(time.milliseconds)

  // 封装着下一条待插入消息的位移信息，包括：
  // 1. 它的 offset
  // 2. 所处的 segment 的 baseOffset
  // 3. 所处的 segment 的日志文件的物理位置
  @volatile private var nextOffsetMetadata: LogOffsetMetadata = _

  /* The earliest offset which is part of an incomplete transaction. This is used to compute the
   * last stable offset (LSO) in ReplicaManager. Note that it is possible that the "true" first unstable offset
   * gets removed from the log (through record or segment deletion). In this case, the first unstable offset
   * will point to the log start offset, which may actually be either part of a completed transaction or not
   * part of a transaction at all. However, since we only use the LSO for the purpose of restricting the
   * read_committed consumer to fetching decided data (i.e. committed, aborted, or non-transactional), this
   * temporary abuse seems justifiable and saves us from scanning the log after deletion to find the first offsets
   * of each ongoing transaction in order to compute a new first unstable offset. It is possible, however,
   * that this could result in disagreement between replicas depending on when they began replicating the log.
   * In the worst case, the LSO could be seen by a consumer to go backwards.
   */
  @volatile private var firstUnstableOffsetMetadata: Option[LogOffsetMetadata] = None

  /* Keep track of the current high watermark in order to ensure that segments containing offsets at or above it are
   * not eligible for deletion. This means that the active segment is only eligible for deletion if the high watermark
   * equals the log end offset (which may never happen for a partition under consistent load). This is needed to
   * prevent the log start offset (which is exposed in fetch responses) from getting ahead of the high watermark.
   */
  @volatile private var highWatermarkMetadata: LogOffsetMetadata = LogOffsetMetadata(logStartOffset)

  /* the actual segments of the log */
  private val segments: ConcurrentNavigableMap[java.lang.Long, LogSegment] = new ConcurrentSkipListMap[java.lang.Long, LogSegment]

  // Visible for testing
  @volatile var leaderEpochCache: Option[LeaderEpochFileCache] = None

  locally {
    // create the log directory if it doesnt exist
    // 创建目录
    Files.createDirectories(dir.toPath)

    // 初始化上面的 leaderEpochCache 变量，包括：
    // 1. 从文件（leader-epoch-checkpoint）中读出 EpochEntry(epoch, startOffset)，保存成一个 treeMap，Map 的 key 是 epoch，value 是 EpochEntry
    // 2. topic\partition 信息
    initializeLeaderEpochCache()

    // 从磁盘上加载 segment 文件，生成  LogSegment 对象 ，放入到上面的 segments map（ConcurrentNavigableMap）中，并返回 nextOffset
    val nextOffset = loadSegments()

    /* Calculate the offset of the next message */
    // 构建下一条带插入消息的 offset 元数据
    nextOffsetMetadata = LogOffsetMetadata(nextOffset, activeSegment.baseOffset, activeSegment.size)

    // 丢弃掉 leader epoch 中的 offset 大于等于 nextOffset 的数据，保证数据合法
    leaderEpochCache.foreach(_.truncateFromEnd(nextOffsetMetadata.messageOffset))


    /**
     * logStartOffset 的来源：log 目录下的 log-start-offset-checkpoint 文件，
     * kafka 在启动加载 log 的时候，会将文件里的内容转化为 <partition,Long> 的 map，
     * 文件的结构如下:
     * -----checkpoint file begin------
     * 0                <- OffsetCheckpointFile.currentVersion
     * 2                <- following entries size
     * tp1  par1  1     <- the format is: TOPIC  PARTITION  OFFSET
     * tp1  par2  2
     * -----checkpoint file end----------
     */
    // 保证数据合法性，取 log-start-offset-checkpoint 文件和 segment 实际数据中，更大的那一个（相当于取他们的交集）
    updateLogStartOffset(math.max(logStartOffset, segments.firstEntry.getValue.baseOffset))

    // The earliest leader epoch may not be flushed during a hard failure. Recover it here.
    // 保证数据合法性，清理掉无用的 leaderEpoch entry(即 offset < logStartOffset 的所有 entry)，
    // 然后更正最早一条，将 entry 的 offset 设置为 logStartOffset
    leaderEpochCache.foreach(_.truncateFromStart(logStartOffset))

    // Any segment loading or recovery code must not use producerStateManager, so that we can build the full state here
    // from scratch.
    // 加载 segment 或者 recovery 都不会使用 producerStateManager，所以这里重新构建 Producer State
    if (!producerStateManager.isEmpty)
      throw new IllegalStateException("Producer state must be empty during log initialization")

    // hasCleanShutdownFile: 如果有这个文件，说明 broker 是正常关闭的。
    loadProducerState(logEndOffset, reloadFromCleanShutdown = hasCleanShutdownFile)
  }

  def dir: File = _dir

  def parentDir: String = _parentDir

  def parentDirFile: File = new File(_parentDir)

  def initFileSize: Int = {
    if (config.preallocate)
      config.segmentSize
    else
      0
  }

  def updateConfig(newConfig: LogConfig): Unit = {
    val oldConfig = this.config
    this.config = newConfig
    val oldRecordVersion = oldConfig.messageFormatVersion.recordVersion
    val newRecordVersion = newConfig.messageFormatVersion.recordVersion
    if (newRecordVersion.precedes(oldRecordVersion))
      warn(s"Record format version has been downgraded from $oldRecordVersion to $newRecordVersion.")
    if (newRecordVersion.value != oldRecordVersion.value)
      initializeLeaderEpochCache()
  }

  private def checkIfMemoryMappedBufferClosed(): Unit = {
    if (isMemoryMappedBufferClosed)
      throw new KafkaStorageException(s"The memory mapped buffer for log of $topicPartition is already closed")
  }

  def highWatermark: Long = highWatermarkMetadata.messageOffset

  /**
   * Update the high watermark to a new offset. The new high watermark will be lower
   * bounded by the log start offset and upper bounded by the log end offset.
   *
   * This is intended to be called when initializing the high watermark or when updating
   * it on a follower after receiving a Fetch response from the leader.
   *
   * @param hw the suggested new value for the high watermark
   * @return the updated high watermark offset
   */
  def updateHighWatermark(hw: Long): Long = {
    updateHighWatermark(LogOffsetMetadata(hw))
  }

  /**
   * Update high watermark with offset metadata. The new high watermark will be lower
   * bounded by the log start offset and upper bounded by the log end offset.
   *
   * 更新 high watermark。注意不能 ＜ logStartOffset，也不能 ≥ logEndOffset
   *
   * @param highWatermarkMetadata the suggested high watermark with offset metadata
   * @return the updated high watermark offset
   */
  def updateHighWatermark(highWatermarkMetadata: LogOffsetMetadata): Long = {
    val endOffsetMetadata = logEndOffsetMetadata
    val newHighWatermarkMetadata = if (highWatermarkMetadata.messageOffset < logStartOffset) {
      LogOffsetMetadata(logStartOffset)
    } else if (highWatermarkMetadata.messageOffset >= endOffsetMetadata.messageOffset) {
      endOffsetMetadata
    } else {
      highWatermarkMetadata
    }

    updateHighWatermarkMetadata(newHighWatermarkMetadata)
    newHighWatermarkMetadata.messageOffset
  }

  /**
   * Update the high watermark to a new value if and only if it is larger than the old value. It is
   * an error to update to a value which is larger than the log end offset.
   *
   * This method is intended to be used by the leader to update the high watermark after follower
   * fetch offsets have been updated.
   *
   * 这个方法主要是用于 leader 副本更新了 follower 副本的 fetch offset 更新之后
   *
   * @return the old high watermark, if updated by the new value
   */
  def maybeIncrementHighWatermark(newHighWatermark: LogOffsetMetadata): Option[LogOffsetMetadata] = {
    if (newHighWatermark.messageOffset > logEndOffset)
      throw new IllegalArgumentException(s"High watermark $newHighWatermark update exceeds current " +
        s"log end offset $logEndOffsetMetadata")

    lock.synchronized {
      // LogOffsetMetadata 中可能只包含一个 absolute offset，而不包含 segmentBaseOffset 和 relativePositionInSegment，
      // 这时候需要去 Log 中查找出该 offset 对应的 segmentBaseOffset 和 relativePositionInSegment
      // 然后重新封装成 LogOffsetMetadata 并返回
      val oldHighWatermark = fetchHighWatermarkMetadata

      // Ensure that the high watermark increases monotonically. We also update the high watermark when the new
      // offset metadata is on a newer segment, which occurs whenever the log is rolled to a new segment.
      // 确保 hw 是单调递增的
      // 当 log 轮转的时候，offset metadata 移到新的 segment，也会更新 hw
      if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset ||
        (oldHighWatermark.messageOffset == newHighWatermark.messageOffset && oldHighWatermark.onOlderSegment(newHighWatermark))) {
        updateHighWatermarkMetadata(newHighWatermark)
        Some(oldHighWatermark)
      } else {
        None
      }
    }
  }

  /**
   * Get the offset and metadata for the current high watermark. If offset metadata is not
   * known, this will do a lookup in the index and cache the result.
   */
  private def fetchHighWatermarkMetadata: LogOffsetMetadata = {
    checkIfMemoryMappedBufferClosed()

    val offsetMetadata = highWatermarkMetadata
    if (offsetMetadata.messageOffsetOnly) {
      lock.synchronized {
        val fullOffset = convertToOffsetMetadataOrThrow(highWatermark)
        updateHighWatermarkMetadata(fullOffset)
        fullOffset
      }
    } else {
      offsetMetadata
    }
  }

  private def updateHighWatermarkMetadata(newHighWatermark: LogOffsetMetadata): Unit = {
    if (newHighWatermark.messageOffset < 0)
      throw new IllegalArgumentException("High watermark offset should be non-negative")

    lock synchronized {
      if (newHighWatermark.messageOffset < highWatermarkMetadata.messageOffset) {
        warn(s"Non-monotonic update of high watermark from $highWatermarkMetadata to $newHighWatermark")
      }

      highWatermarkMetadata = newHighWatermark
      producerStateManager.onHighWatermarkUpdated(newHighWatermark.messageOffset)
      maybeIncrementFirstUnstableOffset()
    }
    trace(s"Setting high watermark $newHighWatermark")
  }

  /**
   * Get the first unstable offset. Unlike the last stable offset, which is always defined,
   * the first unstable offset only exists if there are transactions in progress.
   *
   * @return the first unstable offset, if it exists
   */
  private[log] def firstUnstableOffset: Option[Long] = firstUnstableOffsetMetadata.map(_.messageOffset)

  private def fetchLastStableOffsetMetadata: LogOffsetMetadata = {
    checkIfMemoryMappedBufferClosed()

    // cache the current high watermark to avoid a concurrent update invalidating the range check
    val highWatermarkMetadata = fetchHighWatermarkMetadata

    firstUnstableOffsetMetadata match {
      case Some(offsetMetadata) if offsetMetadata.messageOffset < highWatermarkMetadata.messageOffset =>
        if (offsetMetadata.messageOffsetOnly) {
          lock synchronized {
            val fullOffset = convertToOffsetMetadataOrThrow(offsetMetadata.messageOffset)
            if (firstUnstableOffsetMetadata.contains(offsetMetadata))
              firstUnstableOffsetMetadata = Some(fullOffset)
            fullOffset
          }
        } else {
          offsetMetadata
        }
      case _ => highWatermarkMetadata
    }
  }

  /**
   * The last stable offset (LSO) is defined as the first offset such that all lower offsets have been "decided."
   * Non-transactional messages are considered decided immediately, but transactional messages are only decided when
   * the corresponding COMMIT or ABORT marker is written. This implies that the last stable offset will be equal
   * to the high watermark if there are no transactional messages in the log. Note also that the LSO cannot advance
   * beyond the high watermark.
   */
  def lastStableOffset: Long = {
    firstUnstableOffsetMetadata match {
      case Some(offsetMetadata) if offsetMetadata.messageOffset < highWatermark => offsetMetadata.messageOffset
      case _ => highWatermark
    }
  }

  def lastStableOffsetLag: Long = highWatermark - lastStableOffset

  /**
    * Fully materialize and return an offset snapshot including segment position info. This method will update
    * the LogOffsetMetadata for the high watermark and last stable offset if they are message-only. Throws an
    * offset out of range error if the segment info cannot be loaded.
    */
  def fetchOffsetSnapshot: LogOffsetSnapshot = {
    val lastStable = fetchLastStableOffsetMetadata
    val highWatermark = fetchHighWatermarkMetadata

    LogOffsetSnapshot(
      logStartOffset,
      logEndOffsetMetadata,
      highWatermark,
      lastStable
    )
  }

  private val tags = {
    val maybeFutureTag = if (isFuture) Map("is-future" -> "true") else Map.empty[String, String]
    Map("topic" -> topicPartition.topic, "partition" -> topicPartition.partition.toString) ++ maybeFutureTag
  }

  newGauge(LogMetricNames.NumLogSegments, () => numberOfSegments, tags)
  newGauge(LogMetricNames.LogStartOffset, () => logStartOffset, tags)
  newGauge(LogMetricNames.LogEndOffset, () => logEndOffset, tags)
  newGauge(LogMetricNames.Size, () => size, tags)

  val producerExpireCheck = scheduler.schedule(name = "PeriodicProducerExpirationCheck", fun = () => {
    lock synchronized {
      producerStateManager.removeExpiredProducers(time.milliseconds)
    }
  }, period = producerIdExpirationCheckIntervalMs, delay = producerIdExpirationCheckIntervalMs, unit = TimeUnit.MILLISECONDS)

  /** The name of this log */
  def name  = dir.getName()

  def recordVersion: RecordVersion = config.messageFormatVersion.recordVersion

  private def initializeLeaderEpochCache(): Unit = lock synchronized {

    // 声明一个 leaderEpochFile 对象（leader-epoch-checkpoint 文件）
    val leaderEpochFile = LeaderEpochCheckpointFile.newFile(dir)

    // 从文件中读出 EpochEntry(epoch, startOffset)，保存成一个 treeMap，Map 的 key 是 epoch，value 是 EpochEntry
    def newLeaderEpochFileCache(): LeaderEpochFileCache = {
      // LogDirFailureChannel 是 Kafka 用于管理和处理日志目录的故障。在 broker 执行磁盘 I/O 操作的时候，
      // 如果遇到 I/O 异常（如 IOException），会把出现问题的日志目录名称添加到 LogDirFailureChannel 中，
      // 标记为离线状态，一旦某个日志目录被标记为离线状态，相关线程就收到通知，去处理这个异常。
      // 离线的日志目录会一直保持离线状态 ，直到 Kafka 代理被重启为止
      val checkpointFile = new LeaderEpochCheckpointFile(leaderEpochFile, logDirFailureChannel)
      new LeaderEpochFileCache(topicPartition, () => logEndOffset, checkpointFile)
    }

    // 这里的 record version 和 magic 以及 message format version 是同一个东西，表示 kafka 的数据的版本
    if (recordVersion.precedes(RecordVersion.V2)) {

      // 早期版本，直接删除 leader-epoch-checkpoint 文件
      val currentCache = if (leaderEpochFile.exists())
        Some(newLeaderEpochFileCache())
      else
        None

      if (currentCache.exists(_.nonEmpty))
        warn(s"Deleting non-empty leader epoch cache due to incompatible message format $recordVersion")

      Files.deleteIfExists(leaderEpochFile.toPath)
      leaderEpochCache = None
    } else {
      leaderEpochCache = Some(newLeaderEpochFileCache())
    }
  }

  /**
   * Removes any temporary files found in log directory, and creates a list of all .swap files which could be swapped
   * in place of existing segment(s). For log splitting, we know that any .swap file whose base offset is higher than
   * the smallest offset .clean file could be part of an incomplete split operation. Such .swap files are also deleted
   * by this method.
   *
   * 删除日志目录中所有的临时文件，然后创建一个可用的 swap 文件列表。
   *
   * 日志切分的时候，只要哪个 .swap 文件的 base offset 大于 .clean 文件中的最小 offset，
   * 表示它是由于 incomplete split operation 导致的，这类 .swap 文件也会被删除。
   *
   *
   * @return Set of .swap files that are valid to be swapped in as segment files
   */
  private def removeTempFilesAndCollectSwapFiles(): Set[File] = {

    def deleteIndicesIfExist(baseFile: File, suffix: String = ""): Unit = {
      info(s"Deleting index files with suffix $suffix for baseFile $baseFile")
      val offset = offsetFromFile(baseFile)
      Files.deleteIfExists(Log.offsetIndexFile(dir, offset, suffix).toPath)
      Files.deleteIfExists(Log.timeIndexFile(dir, offset, suffix).toPath)
      Files.deleteIfExists(Log.transactionIndexFile(dir, offset, suffix).toPath)
    }

    val swapFiles = mutable.Set[File]()
    val cleanFiles = mutable.Set[File]()
    var minCleanedFileOffset = Long.MaxValue

    for (file <- dir.listFiles if file.isFile) {
      if (!file.canRead)
        throw new IOException(s"Could not read file $file")
      val filename = file.getName
      if (filename.endsWith(DeletedFileSuffix)) {
        // 删除准备删除但还没删除的文件（以 .deleted 结尾）
        debug(s"Deleting stray temporary file ${file.getAbsolutePath}")
        Files.deleteIfExists(file.toPath)
      } else if (filename.endsWith(CleanedFileSuffix)) {
        // 收集 log compaction 用到的临时文件
        minCleanedFileOffset = Math.min(offsetFromFileName(filename), minCleanedFileOffset)
        cleanFiles += file
      } else if (filename.endsWith(SwapFileSuffix)) {
        // we crashed in the middle of a swap operation, to recover:
        // if a log, delete the index files, complete the swap operation later
        // if an index just delete the index files, they will be rebuilt
        // 如果有 .swap 文件，说明我们在执行 swap operation 的时候宕机了，为了恢复:
        // 如果是 log，就删除索引文件，稍后再做 swap operation
        // 如果是 index(offsetIndex/timeIndex/txnIndex)，直接删了就可以了，稍后会被重建的
        val baseFile = new File(CoreUtils.replaceSuffix(file.getPath, SwapFileSuffix, ""))
        info(s"Found file ${file.getAbsolutePath} from interrupted swap operation.")
        if (isIndexFile(baseFile)) {
          deleteIndicesIfExist(baseFile)
        } else if (isLogFile(baseFile)) {
          deleteIndicesIfExist(baseFile)
          swapFiles += file
        }
      }
    }

    // KAFKA-6264: Delete all .swap files whose base offset is greater than the minimum .cleaned segment offset. Such .swap
    // files could be part of an incomplete split operation that could not complete. See Log#splitOverflowedSegment
    // for more details about the split operation.
    //
    // 删除所有 baseOffset(文件名中的数字) 大于等于 minimum .cleaned segment offset 的 .swap 文件。
    // 这些 .swap 文件可能是由于一些没法成功执行的 split operation 造成的，没的救了，所以直接删掉。
    val (invalidSwapFiles, validSwapFiles) = swapFiles.partition(file => offsetFromFile(file) >= minCleanedFileOffset)
    invalidSwapFiles.foreach { file =>
      debug(s"Deleting invalid swap file ${file.getAbsoluteFile} minCleanedFileOffset: $minCleanedFileOffset")
      val baseFile = new File(CoreUtils.replaceSuffix(file.getPath, SwapFileSuffix, ""))
      deleteIndicesIfExist(baseFile, SwapFileSuffix)
      Files.deleteIfExists(file.toPath)
    }

    // Now that we have deleted all .swap files that constitute an incomplete split operation, let's delete all .clean files
    // 删除 .cleaned 文件
    cleanFiles.foreach { file =>
      debug(s"Deleting stray .clean file ${file.getAbsolutePath}")
      Files.deleteIfExists(file.toPath)
    }

    // 返回有效的可恢复的 swap 文件列表
    validSwapFiles
  }

  /**
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all logs are loaded
   * It is possible that we encounter a segment with index offset overflow in which case the LogSegmentOffsetOverflowException
   * will be thrown. Note that any segments that were opened before we encountered the exception will remain open and the
   * caller is responsible for closing them appropriately, if needed.
   * @throws LogSegmentOffsetOverflowException if the log directory contains a segment with messages that overflow the index offset
   */
  private def loadSegmentFiles(): Unit = {
    // load segments in ascending order because transactional data from one segment may depend on the
    // segments that come before it
    // 因为事务型的数据可能和数据的时间关系有关，因此这里需要按时间顺序加载 segment 数据
    for (file <- dir.listFiles.sortBy(_.getName) if file.isFile) {
      if (isIndexFile(file)) {
        // if it is an index file, make sure it has a corresponding .log file
        // 确保索引文件有对应的(同名的，或者说同 offset 的) log 文件
        val offset = offsetFromFile(file)
        val logFile = Log.logFile(dir, offset)
        if (!logFile.exists) {
          warn(s"Found an orphaned index file ${file.getAbsolutePath}, with no corresponding log file.")
          Files.deleteIfExists(file.toPath)
        }
      } else if (isLogFile(file)) {
        // if it's a log file, load the corresponding log segment

        // 读文件名，确定 baseOffset
        val baseOffset = offsetFromFile(file)
        val timeIndexFileNewlyCreated = !Log.timeIndexFile(dir, baseOffset).exists()

        // 创建 LogSegment 对象
        val segment = LogSegment.open(dir = dir,
          baseOffset = baseOffset,
          config,
          time = time,
          fileAlreadyExists = true)

        try segment.sanityCheck(timeIndexFileNewlyCreated)
        catch {
          case _: NoSuchFileException =>
            error(s"Could not find offset index file corresponding to log file ${segment.log.file.getAbsolutePath}, " +
              "recovering segment and rebuilding index files...")
            recoverSegment(segment)
          case e: CorruptIndexException =>
            warn(s"Found a corrupted index file corresponding to log file ${segment.log.file.getAbsolutePath} due " +
              s"to ${e.getMessage}}, recovering segment and rebuilding index files...")
            recoverSegment(segment)
        }

        // 将 LogSegment 对象添加到 segment map 中
        addSegment(segment)
      }
    }
  }

  /**
   * Recover the given segment.
   * @param segment Segment to recover
   * @param leaderEpochCache Optional cache for updating the leader epoch during recovery
   * @return The number of bytes truncated from the segment
   * @throws LogSegmentOffsetOverflowException if the segment contains messages that cause index offset overflow
   */
  private def recoverSegment(segment: LogSegment,
                             leaderEpochCache: Option[LeaderEpochFileCache] = None): Int = lock synchronized {
    val producerStateManager = new ProducerStateManager(topicPartition, dir, maxProducerIdExpirationMs)
    rebuildProducerState(segment.baseOffset, reloadFromCleanShutdown = false, producerStateManager)
    val bytesTruncated = segment.recover(producerStateManager, leaderEpochCache)
    // once we have recovered the segment's data, take a snapshot to ensure that we won't
    // need to reload the same segment again while recovering another segment.
    producerStateManager.takeSnapshot()
    bytesTruncated
  }

  /**
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all logs
   * are loaded.
   * @throws LogSegmentOffsetOverflowException if the swap file contains messages that cause the log segment offset to
   *                                           overflow. Note that this is currently a fatal exception as we do not have
   *                                           a way to deal with it. The exception is propagated all the way up to
   *                                           KafkaServer#startup which will cause the broker to shut down if we are in
   *                                           this situation. This is expected to be an extremely rare scenario in practice,
   *                                           and manual intervention might be required to get out of it.
   */
  private def completeSwapOperations(swapFiles: Set[File]): Unit = {
    for (swapFile <- swapFiles) {
      val logFile = new File(CoreUtils.replaceSuffix(swapFile.getPath, SwapFileSuffix, ""))
      val baseOffset = offsetFromFile(logFile)

      // 从磁盘中加载 swap segment
      val swapSegment = LogSegment.open(swapFile.getParentFile,
        baseOffset = baseOffset,
        config,
        time = time,
        fileSuffix = SwapFileSuffix)
      info(s"Found log file ${swapFile.getPath} from interrupted swap operation, repairing.")

      // 以 FileChannelRecordBatch 维度遍历 segment 文件，重建所有的 index 文件，并砍掉 log 和 index 末尾的无效 Bytes
      recoverSegment(swapSegment)

      // We create swap files for two cases:
      // (1) Log cleaning where multiple segments are merged into one, and
      // (2) Log splitting where one segment is split into multiple.
      //
      // 两种情况下会创建 swap 文件:
      // 1. log clean(log compaction)的时候，将多个 segment 合并成一个的时候
      // 2. log splitting(segment 中的数据超过 Int.MAX 条)的时候，将一个 segment 拆分成多个 segment
      //
      // Both of these mean that the resultant swap segments be composed of the original set, i.e. the swap segment
      // must fall within the range of existing segment(s). If we cannot find such a segment, it means the deletion
      // of that segment was successful. In such an event, we should simply rename the .swap to .log without having to
      // do a replace with an existing segment.
      //
      // 如果找不到原始的 log 文件，也就不需要 replace 了，直接重命名就可以了
      val oldSegments = logSegments(swapSegment.baseOffset, swapSegment.readNextOffset).filter { segment =>
        segment.readNextOffset > swapSegment.baseOffset
      }
      replaceSegments(Seq(swapSegment), oldSegments.toSeq, isRecoveredSwapFile = true)
    }
  }

  /**
   * Load the log segments from the log files on disk and return the next offset.
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all logs
   * are loaded.
   * @throws LogSegmentOffsetOverflowException if we encounter a .swap file with messages that overflow index offset; or when
   *                                           we find an unexpected number of .log files with overflow
   */
  private def loadSegments(): Long = {
    // first do a pass through the files in the log directory and remove any temporary files
    // and find any interrupted swap operations
    // 清理 .delete 文件: 需要被删除的 log 文件和 index 文件
    // 清理 .cleaned 文件: 在log compaction 过程中宕机，文件中的数据状态未知，需要删除掉
    // 清理所有的无法使用的 .swap 文件: KAFKA-6264
    // 返回可用的 .swap 文件: 即在 log compaction 结束后，替换原文件时宕机而遗留下的文件，只需要再做一次替换操作就可以了
    val swapFiles = removeTempFilesAndCollectSwapFiles()

    // Now do a second pass and load all the log and index files.
    // We might encounter legacy log segments with offset overflow (KAFKA-6264). We need to split such segments. When
    // this happens, restart loading segment files from scratch.
    // 加载日志文件和索引文件
    // 可能会遇到带有偏移量溢出的旧版日志段（KAFKA-6264）。需要将这些日志段拆分出来。当这种情况发生时，从头开始重新加载日志段文件。
    retryOnOffsetOverflow {
      // In case we encounter a segment with offset overflow, the retry logic will split it after which we need to retry
      // loading of segments. In that case, we also need to close all segments that could have been left open in previous
      // call to loadSegmentFiles().
      // 如果遇到带有偏移量溢出的日志段，会抛出 LogSegmentOffsetOverflowException，重试逻辑将会将其拆分。然后我们需要重试加载这些日志段。
      // 在这种情况下，还需要关闭在之前调用 loadSegmentFiles() 时可能已打开的所有日志段。
      logSegments.foreach(_.close())
      segments.clear()
      loadSegmentFiles()
    }

    // Finally, complete any interrupted swap operations. To be crash-safe,
    // log files that are replaced by the swap segment should be renamed to .deleted
    // before the swap file is restored as the new segment file.
    // 最后，完成被中断的交换操作。
    // 为了保证在崩溃情况下的数据安全， 被 swap segment 替换的日志文件应在 swap 文件恢复为新的 segment 文件之前重命名为 .deleted。
    completeSwapOperations(swapFiles)

    if (!dir.getAbsolutePath.endsWith(Log.DeleteDirSuffix)) {

      // 如果日志目录存在，则执行 recoverLog
      val nextOffset = retryOnOffsetOverflow {
        // 根据 log 文件重建所有的 index 文件，并砍掉 log 和 index 末尾的无效 Bytes
        recoverLog()
      }

      // reset the index size of the currently active log segment to allow more entries
      activeSegment.resizeIndexes(config.maxIndexSize)
      nextOffset
    } else {
       // 如果没有可用的目录，则创建一个空的日志文件
       if (logSegments.isEmpty) {
          addSegment(LogSegment.open(dir = dir,
            baseOffset = 0,
            config,
            time = time,
            fileAlreadyExists = false,
            initFileSize = this.initFileSize,
            preallocate = false))
       }
      0
    }
  }

  private def updateLogEndOffset(offset: Long): Unit = {
    nextOffsetMetadata = LogOffsetMetadata(offset, activeSegment.baseOffset, activeSegment.size)

    // Update the high watermark in case it has gotten ahead of the log end offset following a truncation
    // or if a new segment has been rolled and the offset metadata needs to be updated.
    if (highWatermark >= offset) {
      updateHighWatermarkMetadata(nextOffsetMetadata)
    }

    if (this.recoveryPoint > offset) {
      this.recoveryPoint = offset
    }
  }

  private def updateLogStartOffset(offset: Long): Unit = {
    logStartOffset = offset

    if (highWatermark < offset) {
      updateHighWatermark(offset)
    }

    if (this.recoveryPoint < offset) {
      this.recoveryPoint = offset
    }
  }

  /**
   * Recover the log segments and return the next offset after recovery.
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all
   * logs are loaded.
   * @throws LogSegmentOffsetOverflowException if we encountered a legacy segment with offset overflow
   */
  private def recoverLog(): Long = {
    // if we have the clean shutdown marker, skip recovery
    if (!hasCleanShutdownFile) {
      // okay we need to actually recover this log
      val unflushed = logSegments(this.recoveryPoint, Long.MaxValue).iterator
      var truncated = false

      while (unflushed.hasNext && !truncated) {
        val segment = unflushed.next()
        info(s"Recovering unflushed segment ${segment.baseOffset}")
        val truncatedBytes =
          try {
            recoverSegment(segment, leaderEpochCache)
          } catch {
            case _: InvalidOffsetException =>
              val startOffset = segment.baseOffset
              warn("Found invalid offset during recovery. Deleting the corrupt segment and " +
                s"creating an empty one with starting offset $startOffset")
              segment.truncateTo(startOffset)
          }
        if (truncatedBytes > 0) {
          // we had an invalid message, delete all remaining log
          warn(s"Corruption found in segment ${segment.baseOffset}, truncating to offset ${segment.readNextOffset}")
          removeAndDeleteSegments(unflushed.toList,
            asyncDelete = true,
            reason = LogRecovery)
          truncated = true
        }
      }
    }

    if (logSegments.nonEmpty) {
      val logEndOffset = activeSegment.readNextOffset
      if (logEndOffset < logStartOffset) {
        warn(s"Deleting all segments because logEndOffset ($logEndOffset) is smaller than logStartOffset ($logStartOffset). " +
          "This could happen if segment files were deleted from the file system.")
        removeAndDeleteSegments(logSegments,
          asyncDelete = true,
          reason = LogRecovery)
      }
    }

    if (logSegments.isEmpty) {
      // no existing segments, create a new mutable segment beginning at logStartOffset
      addSegment(LogSegment.open(dir = dir,
        baseOffset = logStartOffset,
        config,
        time = time,
        fileAlreadyExists = false,
        initFileSize = this.initFileSize,
        preallocate = config.preallocate))
    }

    recoveryPoint = activeSegment.readNextOffset
    recoveryPoint
  }

  // Rebuild producer state until lastOffset. This method may be called from the recovery code path, and thus must be
  // free of all side-effects, i.e. it must not update any log-specific state.
  private def rebuildProducerState(lastOffset: Long,
                                   reloadFromCleanShutdown: Boolean,
                                   producerStateManager: ProducerStateManager): Unit = lock synchronized {
    checkIfMemoryMappedBufferClosed()
    val messageFormatVersion = config.messageFormatVersion.recordVersion.value
    val segments = logSegments
    val offsetsToSnapshot =
      if (segments.nonEmpty) {
        val nextLatestSegmentBaseOffset = lowerSegment(segments.last.baseOffset).map(_.baseOffset)
        Seq(nextLatestSegmentBaseOffset, Some(segments.last.baseOffset), Some(lastOffset))
      } else {
        Seq(Some(lastOffset))
      }
    info(s"Loading producer state till offset $lastOffset with message format version $messageFormatVersion")

    // We want to avoid unnecessary scanning of the log to build the producer state when the broker is being
    // upgraded. The basic idea is to use the absence of producer snapshot files to detect the upgrade case,
    // but we have to be careful not to assume too much in the presence of broker failures. The two most common
    // upgrade cases in which we expect to find no snapshots are the following:
    //
    // 1. The broker has been upgraded, but the topic is still on the old message format.
    // 2. The broker has been upgraded, the topic is on the new message format, and we had a clean shutdown.
    //
    // If we hit either of these cases, we skip producer state loading and write a new snapshot at the log end
    // offset (see below). The next time the log is reloaded, we will load producer state using this snapshot
    // (or later snapshots). Otherwise, if there is no snapshot file, then we have to rebuild producer state
    // from the first segment.
    if (messageFormatVersion < RecordBatch.MAGIC_VALUE_V2 ||
        (producerStateManager.latestSnapshotOffset.isEmpty && reloadFromCleanShutdown)) {
      // To avoid an expensive scan through all of the segments, we take empty snapshots from the start of the
      // last two segments and the last offset. This should avoid the full scan in the case that the log needs
      // truncation.
      offsetsToSnapshot.flatten.foreach { offset =>
        producerStateManager.updateMapEndOffset(offset)
        producerStateManager.takeSnapshot()
      }
    } else {
      val isEmptyBeforeTruncation = producerStateManager.isEmpty && producerStateManager.mapEndOffset >= lastOffset
      producerStateManager.truncateAndReload(logStartOffset, lastOffset, time.milliseconds())

      // Only do the potentially expensive reloading if the last snapshot offset is lower than the log end
      // offset (which would be the case on first startup) and there were active producers prior to truncation
      // (which could be the case if truncating after initial loading). If there weren't, then truncating
      // shouldn't change that fact (although it could cause a producerId to expire earlier than expected),
      // and we can skip the loading. This is an optimization for users which are not yet using
      // idempotent/transactional features yet.
      if (lastOffset > producerStateManager.mapEndOffset && !isEmptyBeforeTruncation) {
        val segmentOfLastOffset = floorLogSegment(lastOffset)

        logSegments(producerStateManager.mapEndOffset, lastOffset).foreach { segment =>
          val startOffset = Utils.max(segment.baseOffset, producerStateManager.mapEndOffset, logStartOffset)
          producerStateManager.updateMapEndOffset(startOffset)

          if (offsetsToSnapshot.contains(Some(segment.baseOffset)))
            producerStateManager.takeSnapshot()

          val maxPosition = if (segmentOfLastOffset.contains(segment)) {
            Option(segment.translateOffset(lastOffset))
              .map(_.position)
              .getOrElse(segment.size)
          } else {
            segment.size
          }

          val fetchDataInfo = segment.read(startOffset,
            maxSize = Int.MaxValue,
            maxPosition = maxPosition,
            minOneMessage = false)
          if (fetchDataInfo != null)
            loadProducersFromLog(producerStateManager, fetchDataInfo.records)
        }
      }
      producerStateManager.updateMapEndOffset(lastOffset)
      producerStateManager.takeSnapshot()
    }
  }

  private def loadProducerState(lastOffset: Long, reloadFromCleanShutdown: Boolean): Unit = lock synchronized {
    rebuildProducerState(lastOffset, reloadFromCleanShutdown, producerStateManager)
    maybeIncrementFirstUnstableOffset()
  }

  private def loadProducersFromLog(producerStateManager: ProducerStateManager, records: Records): Unit = {
    val loadedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    records.batches.forEach { batch =>
      if (batch.hasProducerId) {
        val maybeCompletedTxn = updateProducers(batch,
          loadedProducers,
          firstOffsetMetadata = None,
          origin = AppendOrigin.Replication)
        maybeCompletedTxn.foreach(completedTxns += _)
      }
    }
    loadedProducers.values.foreach(producerStateManager.update)
    completedTxns.foreach(producerStateManager.completeTxn)
  }

  private[log] def activeProducersWithLastSequence: Map[Long, Int] = lock synchronized {
    producerStateManager.activeProducers.map { case (producerId, producerIdEntry) =>
      (producerId, producerIdEntry.lastSeq)
    }
  }

  private[log] def lastRecordsOfActiveProducers: Map[Long, LastRecord] = lock synchronized {
    producerStateManager.activeProducers.map { case (producerId, producerIdEntry) =>
      val lastDataOffset = if (producerIdEntry.lastDataOffset >= 0 ) Some(producerIdEntry.lastDataOffset) else None
      val lastRecord = LastRecord(lastDataOffset, producerIdEntry.producerEpoch)
      producerId -> lastRecord
    }
  }

  /**
   * Check if we have the "clean shutdown" file
   */
  private def hasCleanShutdownFile: Boolean = new File(dir.getParentFile, CleanShutdownFile).exists()

  /**
   * The number of segments in the log.
   * Take care! this is an O(n) operation.
   */
  def numberOfSegments: Int = segments.size

  /**
   * Close this log.
   * The memory mapped buffer for index files of this log will be left open until the log is deleted.
   */
  def close(): Unit = {
    debug("Closing log")
    lock synchronized {
      checkIfMemoryMappedBufferClosed()
      producerExpireCheck.cancel(true)
      maybeHandleIOException(s"Error while renaming dir for $topicPartition in dir ${dir.getParent}") {
        // We take a snapshot at the last written offset to hopefully avoid the need to scan the log
        // after restarting and to ensure that we cannot inadvertently hit the upgrade optimization
        // (the clean shutdown file is written after the logs are all closed).
        producerStateManager.takeSnapshot()
        logSegments.foreach(_.close())
      }
    }
  }

  /**
   * Rename the directory of the log
   *
   * @throws KafkaStorageException if rename fails
   */
  def renameDir(name: String): Unit = {
    lock synchronized {
      maybeHandleIOException(s"Error while renaming dir for $topicPartition in log dir ${dir.getParent}") {
        val renamedDir = new File(dir.getParent, name)
        Utils.atomicMoveWithFallback(dir.toPath, renamedDir.toPath)
        if (renamedDir != dir) {
          _dir = renamedDir
          _parentDir = renamedDir.getParent
          logSegments.foreach(_.updateParentDir(renamedDir))
          producerStateManager.logDir = dir
          // re-initialize leader epoch cache so that LeaderEpochCheckpointFile.checkpoint can correctly reference
          // the checkpoint file in renamed log directory
          initializeLeaderEpochCache()
        }
      }
    }
  }

  /**
   * Close file handlers used by log but don't write to disk. This is called if the log directory is offline
   */
  def closeHandlers(): Unit = {
    debug("Closing handlers")
    lock synchronized {
      logSegments.foreach(_.closeHandlers())
      isMemoryMappedBufferClosed = true
    }
  }

  /**
   * Append this message set to the active segment of the log, assigning offsets and Partition Leader Epochs
   *
   * @param records The records to append
   * @param origin Declares the origin of the append which affects required validations
   * @param interBrokerProtocolVersion Inter-broker message protocol version
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @return Information about the appended messages including the first and last offset.
   */
  def appendAsLeader(records: MemoryRecords,
                     leaderEpoch: Int,
                     origin: AppendOrigin = AppendOrigin.Client,
                     interBrokerProtocolVersion: ApiVersion = ApiVersion.latestVersion): LogAppendInfo = {
    append(records, origin, interBrokerProtocolVersion, assignOffsets = true, leaderEpoch, ignoreRecordSize = false)
  }

  /**
   * Append this message set to the active segment of the log without assigning offsets or Partition Leader Epochs
   *
   * @param records The records to append
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @return Information about the appended messages including the first and last offset.
   */
  def appendAsFollower(records: MemoryRecords): LogAppendInfo = {
    append(records,
      origin = AppendOrigin.Replication,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      assignOffsets = false,
      leaderEpoch = -1,
      // disable to check the validation of record size since the record is already accepted by leader.
      ignoreRecordSize = true)
  }

  /**
   * Append this message set to the active segment of the log, rolling over to a fresh segment if necessary.
   *
   * 将数据 append 到 active segment 中，如果 segment 中的数据满了，就滚动创建一个新的 segment
   *
   * This method will generally be responsible for assigning offsets to the messages,
   * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
   *
   * @param records The log records to append
   * @param origin Declares the origin of the append which affects required validations
   * @param interBrokerProtocolVersion Inter-broker message protocol version
   * @param assignOffsets Should the log assign offsets to this message set or blindly apply what it is given
   * @param leaderEpoch The partition's leader epoch which will be applied to messages when offsets are assigned on the leader
   * @param ignoreRecordSize true to skip validation of record size.
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @throws OffsetsOutOfOrderException If out of order offsets found in 'records'
   * @throws UnexpectedAppendOffsetException If the first or last offset in append is less than next offset
   * @return Information about the appended messages including the first and last offset.
   */
  private def append(records: MemoryRecords,
                     origin: AppendOrigin,
                     interBrokerProtocolVersion: ApiVersion,
                     assignOffsets: Boolean,
                     leaderEpoch: Int,
                     ignoreRecordSize: Boolean): LogAppendInfo = {
    maybeHandleIOException(s"Error while appending records to $topicPartition in dir ${dir.getParent}") {

      // 校验 CRC、消息大小是否超限。构建 LogAppendInfo，里面保存着这批 records
      // 的 firstOffset, lastOffset, maxTimestamp, offsetOfMaxTimestamp, codec
      // 等信息
      val appendInfo = analyzeAndValidateRecords(records, origin, ignoreRecordSize)

      // return if we have no valid messages or if this is a duplicate of the last appended entry
      // records 中的数据为空，直接退出
      if (appendInfo.shallowCount == 0)
        return appendInfo

      // trim any invalid bytes or partial messages before appending it to the on-disk log
      // 第一步校验的时候，如果某一个 batch 格式异常，会直接报错退出，
      // 所以 invalid bytes 只有可能出现在最后一个 batch 的后面
      // 这里比较 records.sizeInBytes 和 appendInfo.validBytes,
      // 尝试删除末尾的 invalid bytes(如果有的话)
      var validRecords = trimInvalidBytes(records, appendInfo)

      // they are valid, insert them in the log
      lock synchronized {
        checkIfMemoryMappedBufferClosed()

        // leader 副本调用 append 方法追加数据的时候，assignOffsets = true，会给消息分配一个 offset
        // follower 副本调用 append 方法追加数据的时候，assignOffsets = false，会使用消息体中自带的 offset
        if (assignOffsets) {

          // 分配 offset，其实就是 LogEndOffset
          val offset = new LongRef(nextOffsetMetadata.messageOffset)
          appendInfo.firstOffset = Some(offset.value)

          // 更新 offset，并且做进一步的验证，包括：
          // 1. 启用了 compaction的 topic 的消息，必须包含 key
          // 2. 当 magic >= 1 时，如果内部的消息是压缩过的，他们的 offset 必须是从 0 开始递增的
          // 3. 当 magic >= 1 时，校验或修正时间戳
          // 4. DefaultRecordBatch 中声明的 records 数量必须与实际数量一致
          // 如果消息的版本与 topic 中配置的不一样，这个方法还会将消息转换为 topic 的配置的 message format 版本
          // 为了兼容低版本的数据，这个方法里写了巨多关于格式校验和格式转换的处理代码，我们就不细看了
          val now = time.milliseconds
          val validateAndOffsetAssignResult = try {
            LogValidator.validateMessagesAndAssignOffsets(validRecords,
              topicPartition,
              offset,
              time,
              now,
              appendInfo.sourceCodec,
              appendInfo.targetCodec,
              config.compact,
              config.messageFormatVersion.recordVersion.value,
              config.messageTimestampType,
              config.messageTimestampDifferenceMaxMs,
              leaderEpoch,
              origin,
              interBrokerProtocolVersion,
              brokerTopicStats)
          } catch {
            case e: IOException =>
              throw new KafkaException(s"Error validating messages while appending to log $name", e)
          }

          // 拿到了校验结果，补全 appendInfo 信息
          validRecords = validateAndOffsetAssignResult.validatedRecords
          appendInfo.maxTimestamp = validateAndOffsetAssignResult.maxTimestamp
          appendInfo.offsetOfMaxTimestamp = validateAndOffsetAssignResult.shallowOffsetOfMaxTimestamp
          appendInfo.lastOffset = offset.value - 1
          appendInfo.recordConversionStats = validateAndOffsetAssignResult.recordConversionStats
          if (config.messageTimestampType == TimestampType.LOG_APPEND_TIME)
            appendInfo.logAppendTime = now

          // re-validate message sizes if there's a possibility that they have changed (due to re-compression or message
          // format conversion)
          // 上一步验证过程中，可能会做重新压缩或者格式转换，所以再次校验一下消息大小
          if (!ignoreRecordSize && validateAndOffsetAssignResult.messageSizeMaybeChanged) {
            for (batch <- validRecords.batches.asScala) {
              if (batch.sizeInBytes > config.maxMessageSize) {
                // we record the original message set size instead of the trimmed size
                // to be consistent with pre-compression bytesRejectedRate recording
                brokerTopicStats.topicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
                brokerTopicStats.allTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
                throw new RecordTooLargeException(s"Message batch size is ${batch.sizeInBytes} bytes in append to" +
                  s"partition $topicPartition which exceeds the maximum configured size of ${config.maxMessageSize}.")
              }
            }
          }
        } else {
          // we are taking the offsets we are given
          // 采信消息体中的 offset(通常是 follower 副本拉取数据，然后调用 append 方法的场景)

          // 拒绝非递增的消息序列
          if (!appendInfo.offsetsMonotonic)
            throw new OffsetsOutOfOrderException(s"Out of order offsets found in append to $topicPartition: " +
                                                 records.records.asScala.map(_.offset))

          if (appendInfo.firstOrLastOffsetOfFirstBatch < nextOffsetMetadata.messageOffset) {
            // we may still be able to recover if the log is empty
            // one example: fetching from log start offset on the leader which is not batch aligned,
            // which may happen as a result of AdminClient#deleteRecords()
            //
            // 异常情况: 待插入消息的 offset 小于 Log End Offset
            //
            // 一个已知的场景是：通过 AdminClient#deleteRecords() 删除了一部分消息，
            // 导致从 Leader 节点获取日志起始 offset 时，该 offset 可能未对齐到批次的边界
            //
            // 通常来说是没得救的，但是在下面的情况下，还是能救的回来:
            // logEndOffset == log.logStartOffset && firstOffset < logEndOffset && appendInfo.lastOffset >= logEndOffset
            // 即：日志为空，并且，LogStartOffset 落在了待插入数据的中间的时候；
            //
            // 在 append 方法的最外层，会 catch 这个异常，然后对日志从 firstOffset 位置做一次 truncateFullyAndStartAt()
            // 然后重复做一次 append
            //
            // 之所以要在最外层处理，是因为需要先删除当前 segment，然后创建一个新的 segment，并以 firstOffset 作为其 baseOffset
            val firstOffset = appendInfo.firstOffset match {
              case Some(offset) => offset
              case None => records.batches.asScala.head.baseOffset()
            }

            val firstOrLast = if (appendInfo.firstOffset.isDefined) "First offset" else "Last offset of the first batch"
            throw new UnexpectedAppendOffsetException(
              s"Unexpected offset in append to $topicPartition. $firstOrLast " +
              s"${appendInfo.firstOrLastOffsetOfFirstBatch} is less than the next offset ${nextOffsetMetadata.messageOffset}. " +
              s"First 10 offsets in append: ${records.records.asScala.take(10).map(_.offset)}, last offset in" +
              s" append: ${appendInfo.lastOffset}. Log start offset = $logStartOffset",
              firstOffset, appendInfo.lastOffset)
          }
        }

        // update the epoch cache with the epoch stamped onto the message by the leader
        // 更新 leaderEpochCache(如果需要的话)
        validRecords.batches.forEach { batch =>
          if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {
            // 比较入参和 epochCache 中的 item，如果有冲突，根据具体情况，
            // 可能会直接追加一个 item，也有可能截掉一部分 item 然后再追加，
            // 当然也有可能不做任何操作
            maybeAssignEpochStartOffset(batch.partitionLeaderEpoch, batch.baseOffset)
          } else {
            // In partial upgrade scenarios, we may get a temporary regression to the message format. In
            // order to ensure the safety of leader election, we clear the epoch cache so that we revert
            // to truncation by high watermark after the next leader election.
            // 在分步升级的场景下，可能会出现消息格式降级。为了确保选举的正确性，
            // 会清理 epoch cache，以便在下一次选举之后，重新从高水位开始截断。

            // 这是因为V2及以上版本中，使用 partitionLeaderEpoch 来明确的指示 epoch 信息，
            // 而旧版本的消息中不包含 partitionLeaderEpoch，它是以 hw 作为主要的截断依据。
            // 如果不清除缓存，可能会导致领导者选举后无法正确回滚到合适的偏移量。
            // 清除缓存后，系统会回退到基于高水位标记的日志截断（truncation by high watermark）机制，
            // 以确保系统的安全性
            leaderEpochCache.filter(_.nonEmpty).foreach { cache =>
              warn(s"Clearing leader epoch cache after unexpected append with message format v${batch.magic}")
              cache.clearAndFlush()
            }
          }
        }

        // check messages set size may be exceed config.segmentSize
        if (validRecords.sizeInBytes > config.segmentSize) {
          throw new RecordBatchTooLargeException(s"Message batch size is ${validRecords.sizeInBytes} bytes in append " +
            s"to partition $topicPartition, which exceeds the maximum configured segment size of ${config.segmentSize}.")
        }

        // 如果当前 segment 已经满了，就轮转生成一个新的 segment
        val segment = maybeRoll(validRecords.sizeInBytes, appendInfo)

        val logOffsetMetadata = LogOffsetMetadata(
          messageOffset = appendInfo.firstOrLastOffsetOfFirstBatch,
          segmentBaseOffset = segment.baseOffset,
          relativePositionInSegment = segment.size)

        // now that we have valid records, offsets assigned, and timestamps updated, we need to
        // validate the idempotent/transactional state of the producers and collect some metadata
        // 验证事务状态
        val (updatedProducers, completedTxns, maybeDuplicate) = analyzeAndValidateProducerState(
          logOffsetMetadata, validRecords, origin)

        maybeDuplicate.foreach { duplicate =>
          appendInfo.firstOffset = Some(duplicate.firstOffset)
          appendInfo.lastOffset = duplicate.lastOffset
          appendInfo.logAppendTime = duplicate.timestamp
          appendInfo.logStartOffset = logStartOffset
          return appendInfo
        }

        // 实际写入消息
        segment.append(largestOffset = appendInfo.lastOffset,
          largestTimestamp = appendInfo.maxTimestamp,
          shallowOffsetOfMaxTimestamp = appendInfo.offsetOfMaxTimestamp,
          records = validRecords)

        // Increment the log end offset. We do this immediately after the append because a
        // write to the transaction index below may fail and we want to ensure that the offsets
        // of future appends still grow monotonically. The resulting transaction index inconsistency
        // will be cleaned up after the log directory is recovered. Note that the end offset of the
        // ProducerStateManager will not be updated and the last stable offset will not advance
        // if the append to the transaction index fails.
        // 更新 logEndOffset。因为 transaction index 的写入可能失败，而我们又要保证未来写入消息的 offset 递增，
        // 所以在写入消息之后立即更新 LEO。因此产生的 transaction index 不一致将会在日志目录 recovered 的时候被解决。
        // 注意，如果transaction index写入失败时，ProducerStateManager 的 end offset 不会被更新，
        // last stable offset 也不会前进。
        updateLogEndOffset(appendInfo.lastOffset + 1)

        // update the producer state
        // 更新事务状态
        for (producerAppendInfo <- updatedProducers.values) {
          producerStateManager.update(producerAppendInfo)
        }

        // update the transaction index with the true last stable offset. The last offset visible
        // to consumers using READ_COMMITTED will be limited by this value and the high watermark.
        for (completedTxn <- completedTxns) {
          val lastStableOffset = producerStateManager.lastStableOffset(completedTxn)
          segment.updateTxnIndex(completedTxn, lastStableOffset)
          producerStateManager.completeTxn(completedTxn)
        }

        // always update the last producer id map offset so that the snapshot reflects the current offset
        // even if there isn't any idempotent data being written
        // 不太懂，以后再看
        producerStateManager.updateMapEndOffset(appendInfo.lastOffset + 1)

        // update the first unstable offset (which is used to compute LSO)
        maybeIncrementFirstUnstableOffset()

        trace(s"Appended message set with last offset: ${appendInfo.lastOffset}, " +
          s"first offset: ${appendInfo.firstOffset}, " +
          s"next offset: ${nextOffsetMetadata.messageOffset}, " +
          s"and messages: $validRecords")

        if (unflushedMessages >= config.flushInterval)
          flush()

        appendInfo
      }
    }
  }

  def maybeAssignEpochStartOffset(leaderEpoch: Int, startOffset: Long): Unit = {
    leaderEpochCache.foreach { cache =>
      cache.assign(leaderEpoch, startOffset)
    }
  }

  def latestEpoch: Option[Int] = leaderEpochCache.flatMap(_.latestEpoch)

  def endOffsetForEpoch(leaderEpoch: Int): Option[OffsetAndEpoch] = {
    leaderEpochCache.flatMap { cache =>
      val (foundEpoch, foundOffset) = cache.endOffsetFor(leaderEpoch)
      if (foundOffset == EpochEndOffset.UNDEFINED_EPOCH_OFFSET)
        None
      else
        Some(OffsetAndEpoch(foundOffset, foundEpoch))
    }
  }

  private def maybeIncrementFirstUnstableOffset(): Unit = lock synchronized {
    checkIfMemoryMappedBufferClosed()

    val updatedFirstStableOffset = producerStateManager.firstUnstableOffset match {
      case Some(logOffsetMetadata) if logOffsetMetadata.messageOffsetOnly || logOffsetMetadata.messageOffset < logStartOffset =>
        val offset = math.max(logOffsetMetadata.messageOffset, logStartOffset)
        Some(convertToOffsetMetadataOrThrow(offset))
      case other => other
    }

    if (updatedFirstStableOffset != this.firstUnstableOffsetMetadata) {
      debug(s"First unstable offset updated to $updatedFirstStableOffset")
      this.firstUnstableOffsetMetadata = updatedFirstStableOffset
    }
  }

  /**
   * Increment the log start offset if the provided offset is larger.
   */
  def maybeIncrementLogStartOffset(newLogStartOffset: Long, reason: LogStartOffsetIncrementReason): Unit = {
    // We don't have to write the log start offset to log-start-offset-checkpoint immediately.
    // The deleteRecordsOffset may be lost only if all in-sync replicas of this broker are shutdown
    // in an unclean manner within log.flush.start.offset.checkpoint.interval.ms. The chance of this happening is low.
    maybeHandleIOException(s"Exception while increasing log start offset for $topicPartition to $newLogStartOffset in dir ${dir.getParent}") {
      lock synchronized {
        if (newLogStartOffset > highWatermark)
          throw new OffsetOutOfRangeException(s"Cannot increment the log start offset to $newLogStartOffset of partition $topicPartition " +
            s"since it is larger than the high watermark $highWatermark")

        checkIfMemoryMappedBufferClosed()
        if (newLogStartOffset > logStartOffset) {
          updateLogStartOffset(newLogStartOffset)
          info(s"Incremented log start offset to $newLogStartOffset due to $reason")
          leaderEpochCache.foreach(_.truncateFromStart(logStartOffset))
          producerStateManager.truncateHead(newLogStartOffset)
          maybeIncrementFirstUnstableOffset()
        }
      }
    }
  }

  private def analyzeAndValidateProducerState(appendOffsetMetadata: LogOffsetMetadata,
                                              records: MemoryRecords,
                                              origin: AppendOrigin):
  (mutable.Map[Long, ProducerAppendInfo], List[CompletedTxn], Option[BatchMetadata]) = {
    val updatedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    var relativePositionInSegment = appendOffsetMetadata.relativePositionInSegment

    for (batch <- records.batches.asScala) {
      if (batch.hasProducerId) {
        val maybeLastEntry = producerStateManager.lastEntry(batch.producerId)

        // if this is a client produce request, there will be up to 5 batches which could have been duplicated.
        // If we find a duplicate, we return the metadata of the appended batch to the client.
        if (origin == AppendOrigin.Client) {
          maybeLastEntry.flatMap(_.findDuplicateBatch(batch)).foreach { duplicate =>
            return (updatedProducers, completedTxns.toList, Some(duplicate))
          }
        }

        // We cache offset metadata for the start of each transaction. This allows us to
        // compute the last stable offset without relying on additional index lookups.
        val firstOffsetMetadata = if (batch.isTransactional)
          Some(LogOffsetMetadata(batch.baseOffset, appendOffsetMetadata.segmentBaseOffset, relativePositionInSegment))
        else
          None

        val maybeCompletedTxn = updateProducers(batch, updatedProducers, firstOffsetMetadata, origin)
        maybeCompletedTxn.foreach(completedTxns += _)
      }

      relativePositionInSegment += batch.sizeInBytes
    }
    (updatedProducers, completedTxns.toList, None)
  }

  /**
   * Validate the following:
   * <ol>
   * <li> each message matches its CRC
   * <li> each message size is valid (if ignoreRecordSize is false)
   * <li> that the sequence numbers of the incoming record batches are consistent with the existing state and with each other.
   * </ol>
   *
   * Also compute the following quantities:
   * <ol>
   * <li> First offset in the message set
   * <li> Last offset in the message set
   * <li> Number of messages
   * <li> Number of valid bytes
   * <li> Whether the offsets are monotonically increasing
   * <li> Whether any compression codec is used (if many are used, then the last one is given)
   * </ol>
   */
  private def analyzeAndValidateRecords(records: MemoryRecords,
                                        origin: AppendOrigin,
                                        ignoreRecordSize: Boolean): LogAppendInfo = {
    var shallowMessageCount = 0
    var validBytesCount = 0
    var firstOffset: Option[Long] = None
    var lastOffset = -1L
    var sourceCodec: CompressionCodec = NoCompressionCodec
    var monotonic = true
    var maxTimestamp = RecordBatch.NO_TIMESTAMP
    var offsetOfMaxTimestamp = -1L
    var readFirstMessage = false
    var lastOffsetOfFirstBatch = -1L

    for (batch <- records.batches.asScala) {
      // we only validate V2 and higher to avoid potential compatibility issues with older clients

      // origin == AppendOrigin.Client 表示是来自客户端的请求，意味着需要进行全量校验
      // kafka 要求生产者发送的消息，其 baseOffset 必须为0，它其实是一个占位符，之后 Kafka 自己会为消息分配真实的偏移量
      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2 && origin == AppendOrigin.Client && batch.baseOffset != 0)
        throw new InvalidRecordException(s"The baseOffset of the record batch in the append to $topicPartition should " +
          s"be 0, but it is ${batch.baseOffset}")

      // update the first offset if on the first message. For magic versions older than 2, we use the last offset
      // to avoid the need to decompress the data (the last offset can be obtained directly from the wrapper message
      //
      // 找到第一条消息的 offset(即 batch 里面保存的 baseOffset)。
      // 对于早期版本的数据(<v2)，取 baseOffset 需要将 records 转化为 recordBatch,
      // 过程中需要解压全部数据，开销太大。所以只取 lastOffset
      //
      // For magic version 2, we can get the first offset directly from the batch header.
      // When appending to the leader, we will update LogAppendInfo.baseOffset with the correct value. In the follower
      // case, validation will be more lenient.
      // Also indicate whether we have the accurate first offset or not
      if (!readFirstMessage) {
        if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
          firstOffset = Some(batch.baseOffset)
        lastOffsetOfFirstBatch = batch.lastOffset
        readFirstMessage = true
      }

      // 比较每个 batch 的 lastOffset 判断数据 offset 是否单调递增
      if (lastOffset >= batch.lastOffset)
        monotonic = false

      // update the last offset seen
      lastOffset = batch.lastOffset

      // Check if the message sizes are valid.
      // 校验消息大小
      val batchSize = batch.sizeInBytes
      if (!ignoreRecordSize && batchSize > config.maxMessageSize) {
        // 统计消息 reject percentage
        brokerTopicStats.topicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
        brokerTopicStats.allTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
        throw new RecordTooLargeException(s"The record batch size in the append to $topicPartition is $batchSize bytes " +
          s"which exceeds the maximum configured value of ${config.maxMessageSize}.")
      }

      // check the validity of the message by checking CRC
      // 校验 CRC
      if (!batch.isValid) {
        brokerTopicStats.allTopicsStats.invalidMessageCrcRecordsPerSec.mark()
        throw new CorruptRecordException(s"Record is corrupt (stored crc = ${batch.checksum()}) in topic partition $topicPartition.")
      }

      if (batch.maxTimestamp > maxTimestamp) {
        maxTimestamp = batch.maxTimestamp
        offsetOfMaxTimestamp = lastOffset
      }

      shallowMessageCount += 1
      validBytesCount += batchSize

      val messageCodec = CompressionCodec.getCompressionCodec(batch.compressionType.id)
      if (messageCodec != NoCompressionCodec)
        sourceCodec = messageCodec
    }

    // Apply broker-side compression if any
    val targetCodec = BrokerCompressionCodec.getTargetCompressionCodec(config.compressionType, sourceCodec)
    LogAppendInfo(firstOffset, lastOffset, maxTimestamp, offsetOfMaxTimestamp, RecordBatch.NO_TIMESTAMP, logStartOffset,
      RecordConversionStats.EMPTY, sourceCodec, targetCodec, shallowMessageCount, validBytesCount, monotonic, lastOffsetOfFirstBatch)
  }

  private def updateProducers(batch: RecordBatch,
                              producers: mutable.Map[Long, ProducerAppendInfo],
                              firstOffsetMetadata: Option[LogOffsetMetadata],
                              origin: AppendOrigin): Option[CompletedTxn] = {
    val producerId = batch.producerId
    val appendInfo = producers.getOrElseUpdate(producerId, producerStateManager.prepareUpdate(producerId, origin))
    appendInfo.append(batch, firstOffsetMetadata)
  }

  /**
   * Trim any invalid bytes from the end of this message set (if there are any)
   *
   * @param records The records to trim
   * @param info The general information of the message set
   * @return A trimmed message set. This may be the same as what was passed in or it may not.
   */
  private def trimInvalidBytes(records: MemoryRecords, info: LogAppendInfo): MemoryRecords = {
    val validBytes = info.validBytes
    if (validBytes < 0)
      throw new CorruptRecordException(s"Cannot append record batch with illegal length $validBytes to " +
        s"log for $topicPartition. A possible cause is a corrupted produce request.")
    if (validBytes == records.sizeInBytes) {
      records
    } else {
      // trim invalid bytes
      val validByteBuffer = records.buffer.duplicate()
      validByteBuffer.limit(validBytes)
      MemoryRecords.readableRecords(validByteBuffer)
    }
  }

  private def emptyFetchDataInfo(fetchOffsetMetadata: LogOffsetMetadata,
                                 includeAbortedTxns: Boolean): FetchDataInfo = {
    val abortedTransactions =
      if (includeAbortedTxns) Some(List.empty[AbortedTransaction])
      else None
    FetchDataInfo(fetchOffsetMetadata,
      MemoryRecords.EMPTY,
      firstEntryIncomplete = false,
      abortedTransactions = abortedTransactions)
  }

  /**
   * Read messages from the log.
   *
   * @param startOffset The offset to begin reading at
   * @param maxLength The maximum number of bytes to read
   * @param isolation The fetch isolation, which controls the maximum offset we are allowed to read
   * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxLength` (if one exists)
   * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the log start offset
   * @return The fetch data information including fetch starting offset metadata and messages read.
   */
  def read(startOffset: Long,
           maxLength: Int,
           isolation: FetchIsolation,
           minOneMessage: Boolean): FetchDataInfo = {
    maybeHandleIOException(s"Exception while reading from $topicPartition in dir ${dir.getParent}") {
      trace(s"Reading maximum $maxLength bytes at offset $startOffset from log with " +
        s"total length $size bytes")

      val includeAbortedTxns = isolation == FetchTxnCommitted

      // Because we don't use the lock for reading, the synchronization is a little bit tricky.
      // We create the local variables to avoid race conditions with updates to the log.
      // 读操作都没有加锁，所以这里也不打算取巧加锁。而是创建一个本地变量，来防止读到脏数据。
      val endOffsetMetadata = nextOffsetMetadata
      val endOffset = endOffsetMetadata.messageOffset
      // 在 segment map 中，找到 startOffset 所在的那一个 segment
      var segmentEntry = segments.floorEntry(startOffset)

      // return error on attempt to read beyond the log end offset or read below log start offset
      if (startOffset > endOffset || segmentEntry == null || startOffset < logStartOffset)
        throw new OffsetOutOfRangeException(s"Received request for offset $startOffset for partition $topicPartition, " +
          s"but we only have log segments in the range $logStartOffset to $endOffset.")

      val maxOffsetMetadata = isolation match {
        // 表示查询所有日志
        case FetchLogEnd => endOffsetMetadata
        // 表示查询所有 hw 以下的日志，即 consumer 可见的日志
        case FetchHighWatermark => fetchHighWatermarkMetadata
        // 表示查询所有已提交的事务(LSO)的日志
        case FetchTxnCommitted => fetchLastStableOffsetMetadata
      }

      if (startOffset == maxOffsetMetadata.messageOffset) {
        return emptyFetchDataInfo(maxOffsetMetadata, includeAbortedTxns)
      } else if (startOffset > maxOffsetMetadata.messageOffset) {
        val startOffsetMetadata = convertToOffsetMetadataOrThrow(startOffset)
        return emptyFetchDataInfo(startOffsetMetadata, includeAbortedTxns)
      }

      // Do the read on the segment with a base offset less than the target offset
      // but if that segment doesn't contain any messages with an offset greater than that
      // continue to read from successive segments until we get some messages or we reach the end of the log
      //
      // 从 baseOffset 小于 startOffset 的 segment 开始遍历后续的所有 segment，查找消息
      while (segmentEntry != null) {
        val segment = segmentEntry.getValue

        val maxPosition = {
          // Use the max offset position if it is on this segment; otherwise, the segment size is the limit.
          // 1. 如果 maxOffsetMetadata 和我们要找的 offset 在同一个 segment, 就以它的 position 为查找的限制
          // 2. 如果 maxOffsetMetadata 和我们要找的 offset 不在同一个 segment,
          //    则说明 maxOffsetMetadata 在后续的 segment 中，那么就以当前 segment 的 size 为查找的限制
          if (maxOffsetMetadata.segmentBaseOffset == segment.baseOffset) {
            maxOffsetMetadata.relativePositionInSegment
          } else {
            segment.size
          }
        }

        val fetchInfo = segment.read(startOffset, maxLength, maxPosition, minOneMessage)
        if (fetchInfo == null) {
          // 没找到的话，就继续遍历下一个 segment
          segmentEntry = segments.higherEntry(segmentEntry.getKey)
        } else {
          // 找到了就返回
          return if (includeAbortedTxns)
            addAbortedTransactions(startOffset, segmentEntry, fetchInfo)
          else
            fetchInfo
        }
      }

      // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
      // this can happen when all messages with offset larger than start offsets have been deleted.
      // In this case, we will return the empty set with log end offset metadata
      //
      // 走到这里说明尽管 start offset 是在 max offset 的范围内，但是我们遍历完所有的 segment 也没有找到消息。
      // 这种情况是因为所有 offset 大于 start offset 的消息都被删除了。
      // 这时候，返回下一条待插入消息的 metaData(不包含 records)
      FetchDataInfo(nextOffsetMetadata, MemoryRecords.EMPTY)
    }
  }

  private[log] def collectAbortedTransactions(startOffset: Long, upperBoundOffset: Long): List[AbortedTxn] = {
    val segmentEntry = segments.floorEntry(startOffset)
    val allAbortedTxns = ListBuffer.empty[AbortedTxn]
    def accumulator(abortedTxns: List[AbortedTxn]): Unit = allAbortedTxns ++= abortedTxns
    collectAbortedTransactions(logStartOffset, upperBoundOffset, segmentEntry, accumulator)
    allAbortedTxns.toList
  }

  private def addAbortedTransactions(startOffset: Long, segmentEntry: JEntry[JLong, LogSegment],
                                     fetchInfo: FetchDataInfo): FetchDataInfo = {
    val fetchSize = fetchInfo.records.sizeInBytes
    val startOffsetPosition = OffsetPosition(fetchInfo.fetchOffsetMetadata.messageOffset,
      fetchInfo.fetchOffsetMetadata.relativePositionInSegment)
    val upperBoundOffset = segmentEntry.getValue.fetchUpperBoundOffset(startOffsetPosition, fetchSize).getOrElse {
      val nextSegmentEntry = segments.higherEntry(segmentEntry.getKey)
      if (nextSegmentEntry != null)
        nextSegmentEntry.getValue.baseOffset
      else
        logEndOffset
    }

    val abortedTransactions = ListBuffer.empty[AbortedTransaction]
    def accumulator(abortedTxns: List[AbortedTxn]): Unit = abortedTransactions ++= abortedTxns.map(_.asAbortedTransaction)
    collectAbortedTransactions(startOffset, upperBoundOffset, segmentEntry, accumulator)

    FetchDataInfo(fetchOffsetMetadata = fetchInfo.fetchOffsetMetadata,
      records = fetchInfo.records,
      firstEntryIncomplete = fetchInfo.firstEntryIncomplete,
      abortedTransactions = Some(abortedTransactions.toList))
  }

  private def collectAbortedTransactions(startOffset: Long, upperBoundOffset: Long,
                                         startingSegmentEntry: JEntry[JLong, LogSegment],
                                         accumulator: List[AbortedTxn] => Unit): Unit = {
    var segmentEntry = startingSegmentEntry
    while (segmentEntry != null) {
      val searchResult = segmentEntry.getValue.collectAbortedTxns(startOffset, upperBoundOffset)
      accumulator(searchResult.abortedTransactions)
      if (searchResult.isComplete)
        return
      segmentEntry = segments.higherEntry(segmentEntry.getKey)
    }
  }

  /**
   * Get an offset based on the given timestamp
   * The offset returned is the offset of the first message whose timestamp is greater than or equals to the
   * given timestamp.
   *
   * If no such message is found, the log end offset is returned.
   *
   * `NOTE:` OffsetRequest V0 does not use this method, the behavior of OffsetRequest V0 remains the same as before
   * , i.e. it only gives back the timestamp based on the last modification time of the log segments.
   *
   * @param targetTimestamp The given timestamp for offset fetching.
   * @return The offset of the first message whose timestamp is greater than or equals to the given timestamp.
   *         None if no such message is found.
   */
  def fetchOffsetByTimestamp(targetTimestamp: Long): Option[TimestampAndOffset] = {
    maybeHandleIOException(s"Error while fetching offset by timestamp for $topicPartition in dir ${dir.getParent}") {
      debug(s"Searching offset for timestamp $targetTimestamp")

      if (config.messageFormatVersion < KAFKA_0_10_0_IV0 &&
        targetTimestamp != ListOffsetRequest.EARLIEST_TIMESTAMP &&
        targetTimestamp != ListOffsetRequest.LATEST_TIMESTAMP)
        throw new UnsupportedForMessageFormatException(s"Cannot search offsets based on timestamp because message format version " +
          s"for partition $topicPartition is ${config.messageFormatVersion} which is earlier than the minimum " +
          s"required version $KAFKA_0_10_0_IV0")

      // Cache to avoid race conditions. `toBuffer` is faster than most alternatives and provides
      // constant time access while being safe to use with concurrent collections unlike `toArray`.
      val segmentsCopy = logSegments.toBuffer
      // For the earliest and latest, we do not need to return the timestamp.
      if (targetTimestamp == ListOffsetRequest.EARLIEST_TIMESTAMP) {
        // The first cached epoch usually corresponds to the log start offset, but we have to verify this since
        // it may not be true following a message format version bump as the epoch will not be available for
        // log entries written in the older format.
        val earliestEpochEntry = leaderEpochCache.flatMap(_.earliestEntry)
        val epochOpt = earliestEpochEntry match {
          case Some(entry) if entry.startOffset <= logStartOffset => Optional.of[Integer](entry.epoch)
          case _ => Optional.empty[Integer]()
        }
        return Some(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, logStartOffset, epochOpt))
      } else if (targetTimestamp == ListOffsetRequest.LATEST_TIMESTAMP) {
        val latestEpochOpt = leaderEpochCache.flatMap(_.latestEpoch).map(_.asInstanceOf[Integer])
        val epochOptional = Optional.ofNullable(latestEpochOpt.orNull)
        return Some(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, logEndOffset, epochOptional))
      }

      // We need to search the first segment whose largest timestamp is >= the target timestamp if there is one.
      val targetSeg = segmentsCopy.find(_.largestTimestamp >= targetTimestamp)
      targetSeg.flatMap(_.findOffsetByTimestamp(targetTimestamp, logStartOffset))
    }
  }

  def legacyFetchOffsetsBefore(timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    // Cache to avoid race conditions. `toBuffer` is faster than most alternatives and provides
    // constant time access while being safe to use with concurrent collections unlike `toArray`.
    val segments = logSegments.toBuffer
    val lastSegmentHasSize = segments.last.size > 0

    val offsetTimeArray =
      if (lastSegmentHasSize)
        new Array[(Long, Long)](segments.length + 1)
      else
        new Array[(Long, Long)](segments.length)

    for (i <- segments.indices)
      offsetTimeArray(i) = (math.max(segments(i).baseOffset, logStartOffset), segments(i).lastModified)
    if (lastSegmentHasSize)
      offsetTimeArray(segments.length) = (logEndOffset, time.milliseconds)

    var startIndex = -1
    timestamp match {
      case ListOffsetRequest.LATEST_TIMESTAMP =>
        startIndex = offsetTimeArray.length - 1
      case ListOffsetRequest.EARLIEST_TIMESTAMP =>
        startIndex = 0
      case _ =>
        var isFound = false
        debug("Offset time array = " + offsetTimeArray.foreach(o => "%d, %d".format(o._1, o._2)))
        startIndex = offsetTimeArray.length - 1
        while (startIndex >= 0 && !isFound) {
          if (offsetTimeArray(startIndex)._2 <= timestamp)
            isFound = true
          else
            startIndex -= 1
        }
    }

    val retSize = maxNumOffsets.min(startIndex + 1)
    val ret = new Array[Long](retSize)
    for (j <- 0 until retSize) {
      ret(j) = offsetTimeArray(startIndex)._1
      startIndex -= 1
    }
    // ensure that the returned seq is in descending order of offsets
    ret.toSeq.sortBy(-_)
  }

  /**
    * Given a message offset, find its corresponding offset metadata in the log.
    * If the message offset is out of range, throw an OffsetOutOfRangeException
    */
  private def convertToOffsetMetadataOrThrow(offset: Long): LogOffsetMetadata = {
    val fetchDataInfo = read(offset,
      maxLength = 1,
      isolation = FetchLogEnd,
      minOneMessage = false)
    fetchDataInfo.fetchOffsetMetadata
  }

  /**
   * Delete any log segments matching the given predicate function,
   * starting with the oldest segment and moving forward until a segment doesn't match.
   *
   * 从前向后，遍历所有的 segments，删除所有符合 predicate 的 segment
   *
   * @param predicate A function that takes in a candidate log segment and the next higher segment
   *                  (if there is one) and returns true iff it is deletable
   * @return The number of segments deleted
   */
  private def deleteOldSegments(predicate: (LogSegment, Option[LogSegment]) => Boolean,
                                reason: SegmentDeletionReason): Int = {
    lock synchronized {
      val deletable = deletableSegments(predicate)
      if (deletable.nonEmpty)
        deleteSegments(deletable, reason)
      else
        0
    }
  }

  private def deleteSegments(deletable: Iterable[LogSegment], reason: SegmentDeletionReason): Int = {
    maybeHandleIOException(s"Error while deleting segments for $topicPartition in dir ${dir.getParent}") {
      val numToDelete = deletable.size
      if (numToDelete > 0) {
        // we must always have at least one segment, so if we are going to delete all the segments, create a new one first
        if (segments.size == numToDelete)
          roll()
        lock synchronized {
          checkIfMemoryMappedBufferClosed()
          // remove the segments for lookups
          //
          // 删除 segment:
          // 首先调用 segments.remove() 方法，从 segments map 中删除对应的 segment，
          // 然后给 segment 对应的 log|offsetIndex|timeIndex|txnIndex 文件加上后缀 .deleted
          // 然后让异步删除这些文件
          // scheduler.schedule("delete-file", () => deleteSegments(), delay = config.fileDeleteDelayMs)
          removeAndDeleteSegments(deletable, asyncDelete = true, reason)

          // 更新 log start offset
          maybeIncrementLogStartOffset(segments.firstEntry.getValue.baseOffset, SegmentDeletion)
        }
      }
      numToDelete
    }
  }

  /**
   * Find segments starting from the oldest until the user-supplied predicate is false or the segment
   * containing the current high watermark is reached. We do not delete segments with offsets at or beyond
   * the high watermark to ensure that the log start offset can never exceed it. If the high watermark
   * has not yet been initialized, no segments are eligible for deletion.
   *
   * 从最早的 segment 开始遍历，直到传入的 predicate = false 或者达到了包含 high watermark 的 segment。
   * 为了确保 log start offset ≤ high watermark，我们不会删除包含 high watermark 的 segment。
   * 如果 hw 未初始化，则不会删除任何 segment。
   *
   * A final segment that is empty will never be returned (since we would just end up re-creating it).
   *
   * @param predicate A function that takes in a candidate log segment and the next higher segment
   *                  (if there is one) and returns true iff it is deletable
   * @return the segments ready to be deleted
   */
  private def deletableSegments(predicate: (LogSegment, Option[LogSegment]) => Boolean): Iterable[LogSegment] = {
    if (segments.isEmpty) {
      Seq.empty
    } else {
      val deletable = ArrayBuffer.empty[LogSegment]
      var segmentEntry = segments.firstEntry
      while (segmentEntry != null) {
        val segment = segmentEntry.getValue
        val nextSegmentEntry = segments.higherEntry(segmentEntry.getKey)
        val (nextSegment, upperBoundOffset, isLastSegmentAndEmpty) = if (nextSegmentEntry != null)
          (nextSegmentEntry.getValue, nextSegmentEntry.getValue.baseOffset, false)
        else
          (null, logEndOffset, segment.size == 0)

        if (highWatermark >= upperBoundOffset && predicate(segment, Option(nextSegment)) && !isLastSegmentAndEmpty) {
          deletable += segment
          segmentEntry = nextSegmentEntry
        } else {
          segmentEntry = null
        }
      }
      deletable
    }
  }

  /**
   * If topic deletion is enabled, delete any log segments that have either expired due to time based retention
   * or because the log size is > retentionSize.
   *
   * 如果配置文件里的 clean policy 是 delete，则删除超过生存期限的日志，以及日志大小超限的日志
   *
   * Whether or not deletion is enabled, delete any log segments that are before the log start offset
   *
   * 不论 clean policy 是什么，offset 小于 log start offset 的日志全部删除
   */
  def deleteOldSegments(): Int = {
    if (config.delete) {
      deleteRetentionMsBreachedSegments() + deleteRetentionSizeBreachedSegments() + deleteLogStartOffsetBreachedSegments()
    } else {
      deleteLogStartOffsetBreachedSegments()
    }
  }

  private def deleteRetentionMsBreachedSegments(): Int = {
    if (config.retentionMs < 0) return 0
    val startMs = time.milliseconds

    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]): Boolean = {
      startMs - segment.largestTimestamp > config.retentionMs
    }

    deleteOldSegments(shouldDelete, RetentionMsBreach)
  }

  private def deleteRetentionSizeBreachedSegments(): Int = {
    if (config.retentionSize < 0 || size < config.retentionSize) return 0
    var diff = size - config.retentionSize
    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]): Boolean = {
      if (diff - segment.size >= 0) {
        diff -= segment.size
        true
      } else {
        false
      }
    }

    deleteOldSegments(shouldDelete, RetentionSizeBreach)
  }

  private def deleteLogStartOffsetBreachedSegments(): Int = {
    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]): Boolean = {
      nextSegmentOpt.exists(_.baseOffset <= logStartOffset)
    }

    deleteOldSegments(shouldDelete, StartOffsetBreach)
  }

  def isFuture: Boolean = dir.getName.endsWith(Log.FutureDirSuffix)

  /**
   * The size of the log in bytes
   */
  def size: Long = Log.sizeInBytes(logSegments)

  /**
   * The offset metadata of the next message that will be appended to the log
   */
  def logEndOffsetMetadata: LogOffsetMetadata = nextOffsetMetadata

  /**
   * The offset of the next message that will be appended to the log
   */
  def logEndOffset: Long = nextOffsetMetadata.messageOffset

  /**
   * Roll the log over to a new empty log segment if necessary.
   *
   * 以下情况会触发 logSegment 的滚动
   * 1. logSegment 的大小超过 config.segmentSize
   * 2. logSegment 中第一条消息的存在时间超过 config.segmentMs
   * 3. logSegment 大小满了，要超过 Integer.MAX_VALUE 了
   *
   * @param messagesSize The messages set size in bytes.
   * @param appendInfo log append information
   * logSegment will be rolled if one of the following conditions met
   * <ol>
   * <li> The logSegment is full
   * <li> The maxTime has elapsed since the timestamp of first message in the segment (or since the create time if
   * the first message does not have a timestamp)
   * <li> The index is full
   * </ol>
   * @return The currently active segment after (perhaps) rolling to a new segment
   */
  private def maybeRoll(messagesSize: Int, appendInfo: LogAppendInfo): LogSegment = {
    val segment = activeSegment  // segment map 的最后一条
    val now = time.milliseconds

    val maxTimestampInMessages = appendInfo.maxTimestamp
    val maxOffsetInMessages = appendInfo.lastOffset

    // shouldRoll 方法中没有复杂逻辑，就是上面注释中的条件判断
    if (segment.shouldRoll(RollParams(config, appendInfo, messagesSize, now))) {
      debug(s"Rolling new log segment (log_size = ${segment.size}/${config.segmentSize}}, " +
        s"offset_index_size = ${segment.offsetIndex.entries}/${segment.offsetIndex.maxEntries}, " +
        s"time_index_size = ${segment.timeIndex.entries}/${segment.timeIndex.maxEntries}, " +
        s"inactive_time_ms = ${segment.timeWaitedForRoll(now, maxTimestampInMessages)}/${config.segmentMs - segment.rollJitterMs}).")

      /*
        maxOffsetInMessages - Integer.MAX_VALUE is a heuristic value for the first offset in the set of messages.
        Since the offset in messages will not differ by more than Integer.MAX_VALUE, this is guaranteed <= the real
        first offset in the set. Determining the true first offset in the set requires decompression, which the follower
        is trying to avoid during log append. Prior behavior assigned new baseOffset = logEndOffset from old segment.
        This was problematic in the case that two consecutive messages differed in offset by
        Integer.MAX_VALUE.toLong + 2 or more.  In this case, the prior behavior would roll a new log segment whose
        base offset was too low to contain the next message.  This edge case is possible when a replica is recovering a
        highly compacted topic from scratch.
        Note that this is only required for pre-V2 message formats because these do not store the first message offset
        in the header.
      */
      // 日志轮转需要为新日志段设置一个合适的baseOffset。一般来说，baseOffset应该从当前消息集的最小偏移量开始。
      // 但在某些情况下，计算这个偏移量有一定困难；
      // 例如，低版本的消息体中，没有 firstOffset 参数，而 follower 追加消息的时候，
      // 出于性能的考虑，又不会去解压数据，这时候就没法拿到第一条消息的 offset 值了，
      //
      // 此时，选择 maxOffsetInMessages - Integer.MAX_VALUE 的原因是，
      // 暂时没搞太明白，以下是我的理解，不一定正确，可以参考下：
      // 1. 如果 maxOffsetInMessages 小，得到的 result 是负值，轮转的时候，会去取 max(result, LogEndOffset)，以 LogEndOffset 为准
      // 2. 如果 maxOffsetInMessages 很大，得到的 result 是正数，实际消息数量>21e，轮转的时候，会去取 max(result, LogEndOffset)，如果 LogEndOffset 大，以 LogEndOffset 为准
      // 2. 如果 maxOffsetInMessages 很大，得到的 result 是正数，实际消息数量>21e，轮转的时候，会去取 max(result, LogEndOffset)，如果 result 大，以 result 为准，这时候，新 segment 的 baseOffset 会比 LogEndOffset 大，中间会有一段 gap，但是其实没影响，因为查询的时候，会按顺序向后遍历各个 segmet，所以不会有额外的查询负担
      // 3. 如果 maxOffsetInMessages 很大，得到的 result 是正数，实际消息数量<21e，轮转的时候，会去取 max(result, LogEndOffset)，如果 LogEndOffset 大，，以 LogEndOffset 为准
      // 3. 如果 maxOffsetInMessages 很大，得到的 result 是正数，实际消息数量<21e，轮转的时候，会去取 max(result, LogEndOffset)，如果 result 大，以 result 为准，这时候，新 segment 的 baseOffset 会比 LogEndOffset 大，中间会有一段 gap，但是其实没影响，因为查询的时候，会按顺序向后遍历各个 segmet，所以不会有额外的查询负担
      //
      //
      // 如果不这么做，遇到超高压缩的数据的时候，插入索引的时候可能会 overflow(因为 offsetIndex 要求 (offset - baseOffset).toInt)
      // 这么做实际上是在上述情况下，适当的抬高了新 segment 的 baseOffset 值。
      //
      // 参考：
      // https://issues.apache.org/jira/browse/KAFKA-4451
      // https://issues.apache.org/jira/browse/KAFKA-3323
      appendInfo.firstOffset match {
        case Some(firstOffset) => roll(Some(firstOffset))
        case None => roll(Some(maxOffsetInMessages - Integer.MAX_VALUE))
      }
    } else {
      segment
    }
  }

  /**
   * Roll the log over to a new active segment starting with the current logEndOffset.
   * This will trim the index to the exact size of the number of entries it currently contains.
   *
   * @return The newly rolled segment
   */
  def roll(expectedNextOffset: Option[Long] = None): LogSegment = {
    maybeHandleIOException(s"Error while rolling log segment for $topicPartition in dir ${dir.getParent}") {
      val start = time.hiResClockMs()
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        val newOffset = math.max(expectedNextOffset.getOrElse(0L), logEndOffset)
        val logFile = Log.logFile(dir, newOffset)

        if (segments.containsKey(newOffset)) {
          // segment with the same base offset already exists and loaded
          if (activeSegment.baseOffset == newOffset && activeSegment.size == 0) {
            // We have seen this happen (see KAFKA-6388) after shouldRoll() returns true for an
            // active segment of size zero because of one of the indexes is "full" (due to _maxEntries == 0).
            warn(s"Trying to roll a new log segment with start offset $newOffset " +
                 s"=max(provided offset = $expectedNextOffset, LEO = $logEndOffset) while it already " +
                 s"exists and is active with size 0. Size of time index: ${activeSegment.timeIndex.entries}," +
                 s" size of offset index: ${activeSegment.offsetIndex.entries}.")
            removeAndDeleteSegments(Seq(activeSegment), asyncDelete = true, LogRoll)
          } else {
            throw new KafkaException(s"Trying to roll a new log segment for topic partition $topicPartition with start offset $newOffset" +
                                     s" =max(provided offset = $expectedNextOffset, LEO = $logEndOffset) while it already exists. Existing " +
                                     s"segment is ${segments.get(newOffset)}.")
          }
        } else if (!segments.isEmpty && newOffset < activeSegment.baseOffset) {
          throw new KafkaException(
            s"Trying to roll a new log segment for topic partition $topicPartition with " +
            s"start offset $newOffset =max(provided offset = $expectedNextOffset, LEO = $logEndOffset) lower than start offset of the active segment $activeSegment")
        } else {
          val offsetIdxFile = offsetIndexFile(dir, newOffset)
          val timeIdxFile = timeIndexFile(dir, newOffset)
          val txnIdxFile = transactionIndexFile(dir, newOffset)

          for (file <- List(logFile, offsetIdxFile, timeIdxFile, txnIdxFile) if file.exists) {
            warn(s"Newly rolled segment file ${file.getAbsolutePath} already exists; deleting it first")
            Files.delete(file.toPath)
          }

          Option(segments.lastEntry).foreach(_.getValue.onBecomeInactiveSegment())
        }

        // take a snapshot of the producer state to facilitate recovery. It is useful to have the snapshot
        // offset align with the new segment offset since this ensures we can recover the segment by beginning
        // with the corresponding snapshot file and scanning the segment data. Because the segment base offset
        // may actually be ahead of the current producer state end offset (which corresponds to the log end offset),
        // we manually override the state offset here prior to taking the snapshot.
        producerStateManager.updateMapEndOffset(newOffset)
        producerStateManager.takeSnapshot()

        val segment = LogSegment.open(dir,
          baseOffset = newOffset,
          config,
          time = time,
          fileAlreadyExists = false,
          initFileSize = initFileSize,
          preallocate = config.preallocate)
        addSegment(segment)

        // We need to update the segment base offset and append position data of the metadata when log rolls.
        // The next offset should not change.
        updateLogEndOffset(nextOffsetMetadata.messageOffset)

        // schedule an asynchronous flush of the old segment
        scheduler.schedule("flush-log", () => flush(newOffset), delay = 0L)

        info(s"Rolled new log segment at offset $newOffset in ${time.hiResClockMs() - start} ms.")

        segment
      }
    }
  }

  /**
   * The number of messages appended to the log since the last flush
   */
  def unflushedMessages: Long = this.logEndOffset - this.recoveryPoint

  /**
   * Flush all log segments
   */
  def flush(): Unit = flush(this.logEndOffset)

  /**
   * Flush log segments for all offsets up to offset-1
   *
   * @param offset The offset to flush up to (non-inclusive); the new recovery point
   */
  def flush(offset: Long): Unit = {
    maybeHandleIOException(s"Error while flushing log for $topicPartition in dir ${dir.getParent} with offset $offset") {
      if (offset <= this.recoveryPoint)
        return
      debug(s"Flushing log up to offset $offset, last flushed: $lastFlushTime,  current time: ${time.milliseconds()}, " +
        s"unflushed: $unflushedMessages")
      for (segment <- logSegments(this.recoveryPoint, offset))
        segment.flush()

      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        if (offset > this.recoveryPoint) {
          this.recoveryPoint = offset
          lastFlushedTime.set(time.milliseconds)
        }
      }
    }
  }

  /**
   * Cleanup old producer snapshots after the recovery point is checkpointed. It is useful to retain
   * the snapshots from the recent segments in case we need to truncate and rebuild the producer state.
   * Otherwise, we would always need to rebuild from the earliest segment.
   *
   * More specifically:
   *
   * 1. We always retain the producer snapshot from the last two segments. This solves the common case
   * of truncating to an offset within the active segment, and the rarer case of truncating to the previous segment.
   *
   * 2. We only delete snapshots for offsets less than the recovery point. The recovery point is checkpointed
   * periodically and it can be behind after a hard shutdown. Since recovery starts from the recovery point, the logic
   * of rebuilding the producer snapshots in one pass and without loading older segments is simpler if we always
   * have a producer snapshot for all segments being recovered.
   *
   * Return the minimum snapshots offset that was retained.
   */
  def deleteSnapshotsAfterRecoveryPointCheckpoint(): Long = {
    val minOffsetToRetain = minSnapshotsOffsetToRetain
    producerStateManager.deleteSnapshotsBefore(minOffsetToRetain)
    minOffsetToRetain
  }

  // Visible for testing, see `deleteSnapshotsAfterRecoveryPointCheckpoint()` for details
  private[log] def minSnapshotsOffsetToRetain: Long = {
    lock synchronized {
      val twoSegmentsMinOffset = lowerSegment(activeSegment.baseOffset).getOrElse(activeSegment).baseOffset
      // Prefer segment base offset
      val recoveryPointOffset = lowerSegment(recoveryPoint).map(_.baseOffset).getOrElse(recoveryPoint)
      math.min(recoveryPointOffset, twoSegmentsMinOffset)
    }
  }

  private def lowerSegment(offset: Long): Option[LogSegment] =
    Option(segments.lowerEntry(offset)).map(_.getValue)

  /**
   * Completely delete this log directory and all contents from the file system with no delay
   */
  private[log] def delete(): Unit = {
    maybeHandleIOException(s"Error while deleting log for $topicPartition in dir ${dir.getParent}") {
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        producerExpireCheck.cancel(true)
        removeAndDeleteSegments(logSegments, asyncDelete = false, LogDeletion)
        leaderEpochCache.foreach(_.clear())
        Utils.delete(dir)
        // File handlers will be closed if this log is deleted
        isMemoryMappedBufferClosed = true
      }
    }
  }

  // visible for testing
  private[log] def takeProducerSnapshot(): Unit = lock synchronized {
    checkIfMemoryMappedBufferClosed()
    producerStateManager.takeSnapshot()
  }

  // visible for testing
  private[log] def latestProducerSnapshotOffset: Option[Long] = lock synchronized {
    producerStateManager.latestSnapshotOffset
  }

  // visible for testing
  private[log] def oldestProducerSnapshotOffset: Option[Long] = lock synchronized {
    producerStateManager.oldestSnapshotOffset
  }

  // visible for testing
  private[log] def latestProducerStateEndOffset: Long = lock synchronized {
    producerStateManager.mapEndOffset
  }

  /**
   * Truncate this log so that it ends with the greatest offset < targetOffset.
   *
   * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
   * @return True iff targetOffset < logEndOffset
   */
  private[kafka] def truncateTo(targetOffset: Long): Boolean = {
    maybeHandleIOException(s"Error while truncating log to offset $targetOffset for $topicPartition in dir ${dir.getParent}") {
      if (targetOffset < 0)
        throw new IllegalArgumentException(s"Cannot truncate partition $topicPartition to a negative offset (%d).".format(targetOffset))
      if (targetOffset >= logEndOffset) {
        info(s"Truncating to $targetOffset has no effect as the largest offset in the log is ${logEndOffset - 1}")

        // Always truncate epoch cache since we may have a conflicting epoch entry at the
        // end of the log from the leader. This could happen if this broker was a leader
        // and inserted the first start offset entry, but then failed to append any entries
        // before another leader was elected.
        lock synchronized {
          leaderEpochCache.foreach(_.truncateFromEnd(logEndOffset))
        }

        false
      } else {
        info(s"Truncating to offset $targetOffset")
        lock synchronized {
          checkIfMemoryMappedBufferClosed()
          if (segments.firstEntry.getValue.baseOffset > targetOffset) {
            truncateFullyAndStartAt(targetOffset)
          } else {
            val deletable = logSegments.filter(segment => segment.baseOffset > targetOffset)
            removeAndDeleteSegments(deletable, asyncDelete = true, LogTruncation)
            activeSegment.truncateTo(targetOffset)
            leaderEpochCache.foreach(_.truncateFromEnd(targetOffset))

            completeTruncation(
              startOffset = math.min(targetOffset, logStartOffset),
              endOffset = targetOffset
            )
          }
          true
        }
      }
    }
  }

  /**
   *  Delete all data in the log and start at the new offset
   *
   *  @param newOffset The new offset to start the log with
   */
  private[log] def truncateFullyAndStartAt(newOffset: Long): Unit = {
    maybeHandleIOException(s"Error while truncating the entire log for $topicPartition in dir ${dir.getParent}") {
      debug(s"Truncate and start at offset $newOffset")
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        removeAndDeleteSegments(logSegments, asyncDelete = true, LogTruncation)
        addSegment(LogSegment.open(dir,
          baseOffset = newOffset,
          config = config,
          time = time,
          fileAlreadyExists = false,
          initFileSize = initFileSize,
          preallocate = config.preallocate))
        leaderEpochCache.foreach(_.clearAndFlush())
        producerStateManager.truncateFullyAndStartAt(newOffset)

        completeTruncation(
          startOffset = newOffset,
          endOffset = newOffset
        )
      }
    }
  }

  private def completeTruncation(
    startOffset: Long,
    endOffset: Long
  ): Unit = {
    logStartOffset = startOffset
    nextOffsetMetadata = LogOffsetMetadata(endOffset, activeSegment.baseOffset, activeSegment.size)
    recoveryPoint = math.min(recoveryPoint, endOffset)
    rebuildProducerState(endOffset, reloadFromCleanShutdown = false, producerStateManager)
    updateHighWatermark(math.min(highWatermark, endOffset))
  }

  /**
   * The time this log is last known to have been fully flushed to disk
   */
  def lastFlushTime: Long = lastFlushedTime.get

  /**
   * The active segment that is currently taking appends
   */
  def activeSegment = segments.lastEntry.getValue

  /**
   * All the log segments in this log ordered from oldest to newest
   */
  def logSegments: Iterable[LogSegment] = segments.values.asScala

  /**
   * Get all segments beginning with the segment that includes "from" and ending with the segment
   * that includes up to "to-1" or the end of the log (if to > logEndOffset).
   */
  def logSegments(from: Long, to: Long): Iterable[LogSegment] = {
    if (from == to) {
      // Handle non-segment-aligned empty sets
      List.empty[LogSegment]
    } else if (to < from) {
      throw new IllegalArgumentException(s"Invalid log segment range: requested segments in $topicPartition " +
        s"from offset $from which is greater than limit offset $to")
    } else {
      lock synchronized {
        val view = Option(segments.floorKey(from)).map { floor =>
          segments.subMap(floor, to)
        }.getOrElse(segments.headMap(to))
        view.values.asScala
      }
    }
  }

  def nonActiveLogSegmentsFrom(from: Long): Iterable[LogSegment] = {
    lock synchronized {
      if (from > activeSegment.baseOffset)
        Seq.empty
      else
        logSegments(from, activeSegment.baseOffset)
    }
  }

  /**
   * Get the largest log segment with a base offset less than or equal to the given offset, if one exists.
   * @return the optional log segment
   */
  private def floorLogSegment(offset: Long): Option[LogSegment] = {
    Option(segments.floorEntry(offset)).map(_.getValue)
  }

  override def toString: String = {
    val logString = new StringBuilder
    logString.append(s"Log(dir=$dir")
    logString.append(s", topic=${topicPartition.topic}")
    logString.append(s", partition=${topicPartition.partition}")
    logString.append(s", highWatermark=$highWatermark")
    logString.append(s", lastStableOffset=$lastStableOffset")
    logString.append(s", logStartOffset=$logStartOffset")
    logString.append(s", logEndOffset=$logEndOffset")
    logString.append(")")
    logString.toString
  }

  /**
   * This method deletes the given log segments by doing the following for each of them:
   * <ol>
   *   <li>It removes the segment from the segment map so that it will no longer be used for reads.
   *   <li>It renames the index and log files by appending .deleted to the respective file name
   *   <li>It can either schedule an asynchronous delete operation to occur in the future or perform the deletion synchronously
   * </ol>
   * Asynchronous deletion allows reads to happen concurrently without synchronization and without the possibility of
   * physically deleting a file while it is being read.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is either called before all logs are loaded
   * or the immediate caller will catch and handle IOException
   *
   * @param segments The log segments to schedule for deletion
   * @param asyncDelete Whether the segment files should be deleted asynchronously
   */
  private def removeAndDeleteSegments(segments: Iterable[LogSegment],
                                      asyncDelete: Boolean,
                                      reason: SegmentDeletionReason): Unit = {
    if (segments.nonEmpty) {
      lock synchronized {
        // As most callers hold an iterator into the `segments` collection and `removeAndDeleteSegment` mutates it by
        // removing the deleted segment, we should force materialization of the iterator here, so that results of the
        // iteration remain valid and deterministic.
        val toDelete = segments.toList
        reason.logReason(this, toDelete)
        toDelete.foreach { segment =>
          this.segments.remove(segment.baseOffset)
        }
        deleteSegmentFiles(toDelete, asyncDelete)
      }
    }
  }

  /**
   * Perform physical deletion for the given file. Allows the file to be deleted asynchronously or synchronously.
   *
   * 物理删除 segment 相关的文件，支持同步删除和异步删除
   *
   * This method assumes that the file exists and the method is not thread-safe.
   *
   * This method does not need to convert IOException (thrown from changeFileSuffixes) to KafkaStorageException because
   * it is either called before all logs are loaded or the caller will catch and handle IOException
   *
   * @throws IOException if the file can't be renamed and still exists
   */
  private def deleteSegmentFiles(segments: Iterable[LogSegment], asyncDelete: Boolean): Unit = {
    // 改名，给 log|offsetIndex|timeIndex|txnIndex 文件加上后缀 .deleted
    segments.foreach(_.changeFileSuffixes("", Log.DeletedFileSuffix))

    def deleteSegments(): Unit = {
      info(s"Deleting segment files ${segments.mkString(",")}")
      maybeHandleIOException(s"Error while deleting segments for $topicPartition in dir ${dir.getParent}") {
        segments.foreach(_.deleteIfExists())
      }
    }

    if (asyncDelete)
      scheduler.schedule("delete-file", () => deleteSegments(), delay = config.fileDeleteDelayMs)
    else
      deleteSegments()
  }

  /**
   * Swap one or more new segment in place and delete one or more existing segments in a crash-safe manner. The old
   * segments will be asynchronously deleted.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is either called before all logs are loaded
   * or the caller will catch and handle IOException
   *
   * The sequence of operations is:
   * <ol>
   *   <li> Cleaner creates one or more new segments with suffix .cleaned and invokes replaceSegments().
   *        If broker crashes at this point, the clean-and-swap operation is aborted and
   *        the .cleaned files are deleted on recovery in loadSegments().
   *   <li> New segments are renamed .swap. If the broker crashes before all segments were renamed to .swap, the
   *        clean-and-swap operation is aborted - .cleaned as well as .swap files are deleted on recovery in
   *        loadSegments(). We detect this situation by maintaining a specific order in which files are renamed from
   *        .cleaned to .swap. Basically, files are renamed in descending order of offsets. On recovery, all .swap files
   *        whose offset is greater than the minimum-offset .clean file are deleted.
   *   <li> If the broker crashes after all new segments were renamed to .swap, the operation is completed, the swap
   *        operation is resumed on recovery as described in the next step.
   *   <li> Old segment files are renamed to .deleted and asynchronous delete is scheduled.
   *        If the broker crashes, any .deleted files left behind are deleted on recovery in loadSegments().
   *        replaceSegments() is then invoked to complete the swap with newSegment recreated from
   *        the .swap file and oldSegments containing segments which were not renamed before the crash.
   *   <li> Swap segment(s) are renamed to replace the existing segments, completing this operation.
   *        If the broker crashes, any .deleted files which may be left behind are deleted
   *        on recovery in loadSegments().
   * </ol>
   *
   * @param newSegments The new log segment to add to the log
   * @param oldSegments The old log segments to delete from the log
   * @param isRecoveredSwapFile true if the new segment was created from a swap file during recovery after a crash
   */
  private[log] def replaceSegments(newSegments: Seq[LogSegment], oldSegments: Seq[LogSegment], isRecoveredSwapFile: Boolean = false): Unit = {
    lock synchronized {
      val sortedNewSegments = newSegments.sortBy(_.baseOffset)

      // Some old segments may have been removed from index and scheduled for async deletion after the caller reads segments
      // but before this method is executed. We want to filter out those segments to avoid calling asyncDeleteSegment()
      // multiple times for the same segment.
      // 重新校验一下 segment 的存在性
      val sortedOldSegments = oldSegments.filter(seg => segments.containsKey(seg.baseOffset)).sortBy(_.baseOffset)

      checkIfMemoryMappedBufferClosed()
      // need to do this in two phases to be crash safe AND do the delete asynchronously
      // if we crash in the middle of this we complete the swap in loadSegments()
      if (!isRecoveredSwapFile)
        sortedNewSegments.reverse.foreach(_.changeFileSuffixes(Log.CleanedFileSuffix, Log.SwapFileSuffix))
      sortedNewSegments.reverse.foreach(addSegment(_))

      // delete the old files
      for (seg <- sortedOldSegments) {
        // remove the index entry
        if (seg.baseOffset != sortedNewSegments.head.baseOffset)
          segments.remove(seg.baseOffset)
        // delete segment files
        deleteSegmentFiles(List(seg), asyncDelete = true)
      }
      // okay we are safe now, remove the swap suffix
      sortedNewSegments.foreach(_.changeFileSuffixes(Log.SwapFileSuffix, ""))
    }
  }

  /**
    * This function does not acquire Log.lock. The caller has to make sure log segments don't get deleted during
    * this call, and also protects against calling this function on the same segment in parallel.
    *
    * Currently, it is used by LogCleaner threads on log compact non-active segments only with LogCleanerManager's lock
    * to ensure no other logcleaner threads and retention thread can work on the same segment.
    */
  private[log] def getFirstBatchTimestampForSegments(segments: Iterable[LogSegment]): Iterable[Long] = {
    segments.map {
      segment =>
        segment.getFirstBatchTimestamp()
    }
  }

  /**
   * remove deleted log metrics
   */
  private[log] def removeLogMetrics(): Unit = {
    removeMetric(LogMetricNames.NumLogSegments, tags)
    removeMetric(LogMetricNames.LogStartOffset, tags)
    removeMetric(LogMetricNames.LogEndOffset, tags)
    removeMetric(LogMetricNames.Size, tags)
  }

  /**
   * Add the given segment to the segments in this log. If this segment replaces an existing segment, delete it.
   * @param segment The segment to add
   */
  @threadsafe
  def addSegment(segment: LogSegment): LogSegment = this.segments.put(segment.baseOffset, segment)

  private def maybeHandleIOException[T](msg: => String)(fun: => T): T = {
    try {
      fun
    } catch {
      case e: IOException =>
        logDirFailureChannel.maybeAddOfflineLogDir(dir.getParent, msg, e)
        throw new KafkaStorageException(msg, e)
    }
  }

  private[log] def retryOnOffsetOverflow[T](fn: => T): T = {
    while (true) {
      try {
        return fn
      } catch {
        case e: LogSegmentOffsetOverflowException =>
          info(s"Caught segment overflow error: ${e.getMessage}. Split segment and retry.")
          splitOverflowedSegment(e.segment)
      }
    }
    throw new IllegalStateException()
  }

  /**
   * Split a segment into one or more segments such that there is no offset overflow in any of them. The
   * resulting segments will contain the exact same messages that are present in the input segment. On successful
   * completion of this method, the input segment will be deleted and will be replaced by the resulting new segments.
   * See replaceSegments for recovery logic, in case the broker dies in the middle of this operation.
   *
   * 将一个 segment 分割成一个或多个新的 segment，以确保它们中没有任何 offset 溢出。
   * 这个方法执行完之后，输入的 segment 将被删除，并将被新生成的 segment 所替代；
   * 在方法的执行过程中，如果 broker 宕机，可以通过 replaceSegments 来恢复。
   *
   * <p>Note that this method assumes we have already determined that the segment passed in contains records that cause
   * offset overflow.</p>
   *
   * 注意，这个方法假定 segment 确实已经发生了 offset overflow
   *
   * <p>The split logic overloads the use of .clean files that LogCleaner typically uses to make the process of replacing
   * the input segment with multiple new segments atomic and recoverable in the event of a crash. See replaceSegments
   * and completeSwapOperations for the implementation to make this operation recoverable on crashes.</p>
   *
   * 分割逻辑使用了 LogCleaner 通常使用的 .clean 文件的分割逻辑。
   * 这个逻辑用于使用多个新的 segments 替换 input segment 的过程具有原子性，并且在发生崩溃时能够进行恢复。
   * 可以参考 replaceSegments 和 completeSwapOperations 的实现，了解在程序 crash 的情况下可恢复性是如何保证的。
   *
   * @param segment Segment to split
   * @return List of new segments that replace the input segment
   */
  private[log] def splitOverflowedSegment(segment: LogSegment): List[LogSegment] = {
    require(isLogFile(segment.log.file), s"Cannot split file ${segment.log.file.getAbsoluteFile}")
    require(segment.hasOverflow, "Split operation is only permitted for segments with overflow")

    info(s"Splitting overflowed segment $segment")

    val newSegments = ListBuffer[LogSegment]()
    try {
      var position = 0
      val sourceRecords = segment.log

      // 将原 segment 拆分成多个合法的 segment
      // 方法假定入参一定是合法的(segment 一定是 overflow)，所以原 segment 至少会被拆成两个 newSegment
      while (position < sourceRecords.sizeInBytes) {
        val firstBatch = sourceRecords.batchesFrom(position).asScala.head
        // 为当前新建一个临时 segment 文件，后缀是 .cleaned
        val newSegment = LogCleaner.createNewCleanedSegment(this, firstBatch.baseOffset)
        newSegments += newSegment

        // 根据 position 将 segment 中的内容写到 newSegment 中
        val bytesAppended = newSegment.appendFromFile(sourceRecords, position)
        if (bytesAppended == 0)
          throw new IllegalStateException(s"Failed to append records from position $position in $segment")

        // 累加 position，循环，直到 segment 中的所有内容都被写到 newSegment 中
        position += bytesAppended
      }

      // prepare new segments
      var totalSizeOfNewSegments = 0
      newSegments.foreach { splitSegment =>
        splitSegment.onBecomeInactiveSegment()
        splitSegment.flush()
        splitSegment.lastModified = segment.lastModified
        totalSizeOfNewSegments += splitSegment.log.sizeInBytes
      }
      // size of all the new segments combined must equal size of the original segment
      if (totalSizeOfNewSegments != segment.log.sizeInBytes)
        throw new IllegalStateException("Inconsistent segment sizes after split" +
          s" before: ${segment.log.sizeInBytes} after: $totalSizeOfNewSegments")

      // replace old segment with new ones
      info(s"Replacing overflowed segment $segment with split segments $newSegments")
      replaceSegments(newSegments.toList, List(segment))
      newSegments.toList
    } catch {
      case e: Exception =>
        newSegments.foreach { splitSegment =>
          splitSegment.close()
          splitSegment.deleteIfExists()
        }
        throw e
    }
  }
}

/**
 * Helper functions for logs
 */
object Log {

  /** a log file */
  val LogFileSuffix = ".log"

  /** an index file */
  val IndexFileSuffix = ".index"

  /** a time index file */
  val TimeIndexFileSuffix = ".timeindex"

  val ProducerSnapshotFileSuffix = ".snapshot"

  /** an (aborted) txn index */
  val TxnIndexFileSuffix = ".txnindex"

  /** a file that is scheduled to be deleted */
  val DeletedFileSuffix = ".deleted"

  /** A temporary file that is being used for log cleaning */
  val CleanedFileSuffix = ".cleaned"

  /** A temporary file used when swapping files into the log */
  val SwapFileSuffix = ".swap"

  /** Clean shutdown file that indicates the broker was cleanly shutdown in 0.8 and higher.
   * This is used to avoid unnecessary recovery after a clean shutdown. In theory this could be
   * avoided by passing in the recovery point, however finding the correct position to do this
   * requires accessing the offset index which may not be safe in an unclean shutdown.
   * For more information see the discussion in PR#2104
   */
  val CleanShutdownFile = ".kafka_cleanshutdown"

  /** a directory that is scheduled to be deleted */
  val DeleteDirSuffix = "-delete"

  /** a directory that is used for future partition */
  val FutureDirSuffix = "-future"

  private[log] val DeleteDirPattern = Pattern.compile(s"^(\\S+)-(\\S+)\\.(\\S+)$DeleteDirSuffix")
  private[log] val FutureDirPattern = Pattern.compile(s"^(\\S+)-(\\S+)\\.(\\S+)$FutureDirSuffix")

  val UnknownOffset = -1L

  def apply(dir: File,
            config: LogConfig,
            logStartOffset: Long,
            recoveryPoint: Long,
            scheduler: Scheduler,
            brokerTopicStats: BrokerTopicStats,
            time: Time = Time.SYSTEM,
            maxProducerIdExpirationMs: Int,
            producerIdExpirationCheckIntervalMs: Int,
            logDirFailureChannel: LogDirFailureChannel): Log = {
    val topicPartition = Log.parseTopicPartitionName(dir)
    val producerStateManager = new ProducerStateManager(topicPartition, dir, maxProducerIdExpirationMs)
    new Log(dir, config, logStartOffset, recoveryPoint, scheduler, brokerTopicStats, time, maxProducerIdExpirationMs,
      producerIdExpirationCheckIntervalMs, topicPartition, producerStateManager, logDirFailureChannel)
  }

  /**
   * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
   * so that ls sorts the files numerically.
   *
   * @param offset The offset to use in the file name
   * @return The filename
   */
  def filenamePrefixFromOffset(offset: Long): String = {
    val nf = NumberFormat.getInstance()
    nf.setMinimumIntegerDigits(20)
    nf.setMaximumFractionDigits(0)
    nf.setGroupingUsed(false)
    nf.format(offset)
  }

  /**
   * Construct a log file name in the given dir with the given base offset and the given suffix
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name (e.g. "", ".deleted", ".cleaned", ".swap", etc.)
   */
  def logFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix + suffix)

  /**
   * Return a directory name to rename the log directory to for async deletion.
   * The name will be in the following format: "topic-partitionId.uniqueId-delete".
   * If the topic name is too long, it will be truncated to prevent the total name
   * from exceeding 255 characters.
   */
  def logDeleteDirName(topicPartition: TopicPartition): String = {
    val uniqueId = java.util.UUID.randomUUID.toString.replaceAll("-", "")
    val suffix = s"-${topicPartition.partition()}.${uniqueId}${DeleteDirSuffix}"
    val prefixLength = Math.min(topicPartition.topic().size, 255 - suffix.size)
    s"${topicPartition.topic().substring(0, prefixLength)}${suffix}"
  }

  /**
   * Return a future directory name for the given topic partition. The name will be in the following
   * format: topic-partition.uniqueId-future where topic, partition and uniqueId are variables.
   */
  def logFutureDirName(topicPartition: TopicPartition): String = {
    logDirNameWithSuffix(topicPartition, FutureDirSuffix)
  }

  private def logDirNameWithSuffix(topicPartition: TopicPartition, suffix: String): String = {
    val uniqueId = java.util.UUID.randomUUID.toString.replaceAll("-", "")
    s"${logDirName(topicPartition)}.$uniqueId$suffix"
  }

  /**
   * Return a directory name for the given topic partition. The name will be in the following
   * format: topic-partition where topic, partition are variables.
   */
  def logDirName(topicPartition: TopicPartition): String = {
    s"${topicPartition.topic}-${topicPartition.partition}"
  }

  /**
   * Construct an index file name in the given dir using the given base offset and the given suffix
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
   */
  def offsetIndexFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix + suffix)

  /**
   * Construct a time index file name in the given dir using the given base offset and the given suffix
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
   */
  def timeIndexFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + TimeIndexFileSuffix + suffix)

  def deleteFileIfExists(file: File, suffix: String = ""): Unit =
    Files.deleteIfExists(new File(file.getPath + suffix).toPath)

  /**
   * Construct a producer id snapshot file using the given offset.
   *
   * @param dir The directory in which the log will reside
   * @param offset The last offset (exclusive) included in the snapshot
   */
  def producerSnapshotFile(dir: File, offset: Long): File =
    new File(dir, filenamePrefixFromOffset(offset) + ProducerSnapshotFileSuffix)

  /**
   * Construct a transaction index file name in the given dir using the given base offset and the given suffix
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
   */
  def transactionIndexFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + TxnIndexFileSuffix + suffix)

  def offsetFromFileName(filename: String): Long = {
    filename.substring(0, filename.indexOf('.')).toLong
  }

  def offsetFromFile(file: File): Long = {
    offsetFromFileName(file.getName)
  }

  /**
   * Calculate a log's size (in bytes) based on its log segments
   *
   * @param segments The log segments to calculate the size of
   * @return Sum of the log segments' sizes (in bytes)
   */
  def sizeInBytes(segments: Iterable[LogSegment]): Long =
    segments.map(_.size.toLong).sum

  /**
   * Parse the topic and partition out of the directory name of a log
   */
  def parseTopicPartitionName(dir: File): TopicPartition = {
    if (dir == null)
      throw new KafkaException("dir should not be null")

    def exception(dir: File): KafkaException = {
      new KafkaException(s"Found directory ${dir.getCanonicalPath}, '${dir.getName}' is not in the form of " +
        "topic-partition or topic-partition.uniqueId-delete (if marked for deletion).\n" +
        "Kafka's log directories (and children) should only contain Kafka topic data.")
    }

    val dirName = dir.getName
    if (dirName == null || dirName.isEmpty || !dirName.contains('-'))
      throw exception(dir)
    if (dirName.endsWith(DeleteDirSuffix) && !DeleteDirPattern.matcher(dirName).matches ||
        dirName.endsWith(FutureDirSuffix) && !FutureDirPattern.matcher(dirName).matches)
      throw exception(dir)

    val name: String =
      if (dirName.endsWith(DeleteDirSuffix) || dirName.endsWith(FutureDirSuffix)) dirName.substring(0, dirName.lastIndexOf('.'))
      else dirName

    val index = name.lastIndexOf('-')
    val topic = name.substring(0, index)
    val partitionString = name.substring(index + 1)
    if (topic.isEmpty || partitionString.isEmpty)
      throw exception(dir)

    val partition =
      try partitionString.toInt
      catch { case _: NumberFormatException => throw exception(dir) }

    new TopicPartition(topic, partition)
  }

  private def isIndexFile(file: File): Boolean = {
    val filename = file.getName
    filename.endsWith(IndexFileSuffix) || filename.endsWith(TimeIndexFileSuffix) || filename.endsWith(TxnIndexFileSuffix)
  }

  private def isLogFile(file: File): Boolean =
    file.getPath.endsWith(LogFileSuffix)

}

object LogMetricNames {
  val NumLogSegments: String = "NumLogSegments"
  val LogStartOffset: String = "LogStartOffset"
  val LogEndOffset: String = "LogEndOffset"
  val Size: String = "Size"

  def allMetricNames: List[String] = {
    List(NumLogSegments, LogStartOffset, LogEndOffset, Size)
  }
}

sealed trait SegmentDeletionReason {
  def logReason(log: Log, toDelete: List[LogSegment]): Unit
}

case object RetentionMsBreach extends SegmentDeletionReason {
  override def logReason(log: Log, toDelete: List[LogSegment]): Unit = {
    val retentionMs = log.config.retentionMs
    toDelete.foreach { segment =>
      segment.largestRecordTimestamp match {
        case Some(_) =>
          log.info(s"Deleting segment $segment due to retention time ${retentionMs}ms breach based on the largest " +
            s"record timestamp in the segment")
        case None =>
          log.info(s"Deleting segment $segment due to retention time ${retentionMs}ms breach based on the " +
            s"last modified time of the segment")
      }
    }
  }
}

case object RetentionSizeBreach extends SegmentDeletionReason {
  override def logReason(log: Log, toDelete: List[LogSegment]): Unit = {
    var size = log.size
    toDelete.foreach { segment =>
      size -= segment.size
      log.info(s"Deleting segment $segment due to retention size ${log.config.retentionSize} breach. Log size " +
        s"after deletion will be $size.")
    }
  }
}

case object StartOffsetBreach extends SegmentDeletionReason {
  override def logReason(log: Log, toDelete: List[LogSegment]): Unit = {
    log.info(s"Deleting segments due to log start offset ${log.logStartOffset} breach: ${toDelete.mkString(",")}")
  }
}

case object LogRecovery extends SegmentDeletionReason {
  override def logReason(log: Log, toDelete: List[LogSegment]): Unit = {
    log.info(s"Deleting segments as part of log recovery: ${toDelete.mkString(",")}")
  }
}

case object LogTruncation extends SegmentDeletionReason {
  override def logReason(log: Log, toDelete: List[LogSegment]): Unit = {
    log.info(s"Deleting segments as part of log truncation: ${toDelete.mkString(",")}")
  }
}

case object LogRoll extends SegmentDeletionReason {
  override def logReason(log: Log, toDelete: List[LogSegment]): Unit = {
    log.info(s"Deleting segments as part of log roll: ${toDelete.mkString(",")}")
  }
}

case object LogDeletion extends SegmentDeletionReason {
  override def logReason(log: Log, toDelete: List[LogSegment]): Unit = {
    log.info(s"Deleting segments as the log has been deleted: ${toDelete.mkString(",")}")
  }
}