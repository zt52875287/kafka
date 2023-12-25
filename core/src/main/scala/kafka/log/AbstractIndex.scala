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

import java.io.{Closeable, File, RandomAccessFile}
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.concurrent.locks.{Lock, ReentrantLock}

import kafka.common.IndexOffsetOverflowException
import kafka.log.IndexSearchType.IndexSearchEntity
import kafka.utils.CoreUtils.inLock
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.utils.{ByteBufferUnmapper, OperatingSystem, Utils}

/**
 * The abstract index class which holds entry format agnostic methods.
 *
 * @param _file The index file
 * @param baseOffset the base offset of the segment that this index is corresponding to.
 * @param maxIndexSize The maximum index size in bytes.
 */
abstract class AbstractIndex(@volatile private var _file: File, val baseOffset: Long, val maxIndexSize: Int = -1,
                             val writable: Boolean) extends Closeable {
  import AbstractIndex._

  // Length of the index file
  @volatile
  private var _length: Long = _
  protected def entrySize: Int

  /*
   Kafka mmaps index files into memory, and all the read / write operations of the index is through OS page cache. This
   avoids blocked disk I/O in most cases.
   Kafka mmaps 将索引文件映射到内存中，索引的所有读写操作都通过操作系统的 page cache 进行。这避免了大多数场景下的阻塞磁盘I/O。

   To the extent of our knowledge, all the modern operating systems use LRU policy or its variants to manage page
   cache. Kafka always appends to the end of the index file, and almost all the index lookups (typically from in-sync
   followers or consumers) are very close to the end of the index. So, the LRU cache replacement policy should work very
   well with Kafka's index access pattern.
   据我们所知，所有现代操作系统都使用 LRU 策略或其变体来管理 page cache。Kafka 始终将数据追加到索引文件的末尾，而几乎所有的索引查找
   （通常来自 in-sync followers 或者 consumers）都非常接近索引的末尾。因此，LRU 应该很好地适应Kafka的索引访问模式。

   However, when looking up index, the standard binary search algorithm is not cache friendly, and can cause unnecessary
   page faults (the thread is blocked to wait for reading some index entries from hard disk, as those entries are not
   cached in the page cache).
   然而，在查找索引时，标准的二分查找对缓存并不友好，还有可能导致不必要的 page faults（线程由于等待一些未在 page cache 中（实际是在硬盘）的索引条目而被阻塞）。

   For example, in an index with 13 pages, to lookup an entry in the last page (page #12), the standard binary search
   algorithm will read index entries in page #0, 6, 9, 11, and 12.
   例如，在一个包含13个页的索引中，查找最后一页中的条目，标准的二分查找将读取第0、6、9、11、12页的索引条目。

   page number: |0|1|2|3|4|5|6|7|8|9|10|11|12 |
   steps:       |1| | | | | |3| | |4|  |5 |2/6|

   In each page, there are hundreds log entries, corresponding to hundreds to thousands of kafka messages. When the
   index gradually growing from the 1st entry in page #12 to the last entry in page #12, all the write (append)
   operations are in page #12, and all the in-sync follower / consumer lookups read page #0,6,9,11,12. As these pages
   are always used in each in-sync lookup, we can assume these pages are fairly recently used, and are very likely to be
   in the page cache. When the index grows to page #13, the pages needed in a in-sync lookup change to #0, 7, 10, 12,
   and 13:
   在每一页中，有数百个日志条目，对应于几百上千条Kafka消息。当索引从第12页的第一个条目逐渐增长到第12页的最后一个条目时，
   所有的写入（追加）操作都在第12页，而所有的 in-sync follower / consumer 查找时都要读取第0、6、9、11、12页。
   当索引增长到第13页时， in-sync 查找所需的页面变为第0、7、10、12和13页：

   page number: |0|1|2|3|4|5|6|7|8|9|10|11|12|13 |
   steps:       |1| | | | | | |3| | | 4|5 | 6|2/7|

   Page #7 and page #10 have not been used for a very long time. They are much less likely to be in the page cache, than
   the other pages. The 1st lookup, after the 1st index entry in page #13 is appended, is likely to have to read page #7
   and page #10 from disk (page fault), which can take up to more than a second. In our test, this can cause the
   at-least-once produce latency to jump to about 1 second from a few ms.
   第7页和第10页长时间未被使用。相比其他页，它们不太可能存在于 page cache。而在第13页的第一个索引条目附加后的第一个查找，
   可能必须从磁盘读取第7页和第10页，这可能需要超过一秒。在我们的测试中，这会导致至少一次秒级的 produce 延迟。

   Here, we use a more cache-friendly lookup algorithm:
   所以我们使用一种对缓存更友好的查找算法：
   if (target > indexEntry[end - N]) // if the target is in the last N entries of the index 如果查询的目标在索引的最后 N 个 entries 中
      binarySearch(end - N, end)
   else
      binarySearch(begin, end - N)

   If possible, we only look up in the last N entries of the index. By choosing a proper constant N, all the in-sync
   lookups should go to the 1st branch. We call the last N entries the "warm" section. As we frequently look up in this
   relatively small section, the pages containing this section are more likely to be in the page cache.
   如果可能，我们只在索引的最后N个条目中查找。通过选择适当的常数N，所有的同步查找应该都进入第一个分支。我们称最后N个条目为“热”部分。
   由于我们经常在这个相对较小的部分中查找，包含该部分的 page 更有可能存在于 page cache 中。

   We set N (_warmEntries) to 8192, because
   我们将 N 设置为 8192，因为：
   1. 这个数字足够小，可以确保每次 warm 查询，都会访问所有的 warm-section。这样，warm-section 才配被称为 warm。
      在 warm-section 查找时，会始终访问下面三个 entry：
      indexEntry(end)
      indexEntry(end-N)
      indexEntry((end*2 -N)/2)
      如果页面大小>=4096，当我们触及这3个条目时，所有 warm-section page（3个或更少（最后一页并不总是满的，所以是3个或更少））都会被访问。截至2018年，
      4096是所有处理器（x86-32、x86-64、MIPS、SPARC、Power、ARM等）的最小页面大小。
   2. 这个数字足够大，以确保大多数 in-sync lookups 都在 warm-section 进行。
      在Kafka默认设置下，8KB索引对应于约4MB（offset index）或2.7MB（time index）的日志消息。

   1. This number is small enough to guarantee all the pages of the "warm" section is touched in every warm-section
      lookup. So that, the entire warm section is really "warm".
      When doing warm-section lookup, following 3 entries are always touched: indexEntry(end), indexEntry(end-N),
      and indexEntry((end*2 -N)/2). If page size >= 4096, all the warm-section pages (3 or fewer) are touched, when we
      touch those 3 entries. As of 2018, 4096 is the smallest page size for all the processors (x86-32, x86-64, MIPS,
      SPARC, Power, ARM etc.).
   2. This number is large enough to guarantee most of the in-sync lookups are in the warm-section. With default Kafka
      settings, 8KB index corresponds to about 4MB (offset index) or 2.7MB (time index) log messages.

   We can't set make N (_warmEntries) to be larger than 8192, as there is no simple way to guarantee all the "warm"
   section pages are really warm (touched in every lookup) on a typical 4KB-page host.
   我们不能将N（_warmEntries）设置为大于8192，因为在典型的4KB页面主机上，没有简单的方法来确保所有 "warm"
   section pages 都是真正 warm 的（在每次查找中都被访问）。

   In there future, we may use a backend thread to periodically touch the entire warm section. So that, we can
   1) support larger warm section
   2) make sure the warm section of low QPS topic-partitions are really warm.
   在将来，我们可能会使用后台线程定期访问整个“热”部分。这样，我们可以：
   1）支持更大的“热”部分
   2）确保低 QPS 主题分区的 warm section 确实是“热”的。
 */
  protected def _warmEntries: Int = 8192 / entrySize

  protected val lock = new ReentrantLock

  @volatile
  protected var mmap: MappedByteBuffer = {
    val newlyCreated = file.createNewFile()
    val raf = if (writable) new RandomAccessFile(file, "rw") else new RandomAccessFile(file, "r")
    try {
      /* pre-allocate the file if necessary */
      if(newlyCreated) {
        if(maxIndexSize < entrySize)
          throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize)
        // 将 maxIndexSize 变为 entrySize 的整数倍，设置为 mmap 的 limit(大小)
        raf.setLength(roundDownToExactMultiple(maxIndexSize, entrySize))
      }

      /* memory-map the file */
      _length = raf.length()
      val idx = {
        // FileChannel.map 方法： FileChannelImpl 中通过反射的方式，调用：
        // java.nio.DirectByteBuffer.DirectByteBuffer(int, long, java.io.FileDescriptor, java.lang.Runnable)
        // 构造出来 MappedByteBuffer。
        //
        // length 来自于当前对象 AbstractIndex 的构造方法的入参，对应 kafka 配置项：log.index.size.max.bytes
        // 它的默认值 LogIndexSizeMaxBytes = 10*1024*1024
        //
        // 我们据此可已计算出：
        // OffsetIndex 最多可以存储 10*1024*1024/8 = 1310720 条数据
        // TimeIndex 最多可以存储 10*1024*1024(先缩减到12的倍数)/12 = 873813 条数据
        if (writable)
          raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, _length)
        else
          raf.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, _length)
      }
      /* set the position in the index for the next entry */
      if(newlyCreated)
        idx.position(0)
      else
        // if this is a pre-existing index, assume it is valid and set position to last entry
        idx.position(roundDownToExactMultiple(idx.limit(), entrySize))
      idx
    } finally {
      CoreUtils.swallow(raf.close(), AbstractIndex)
    }
  }

  /**
   * The maximum number of entries this index can hold
   */
  @volatile
  private[this] var _maxEntries: Int = mmap.limit() / entrySize

  /** The number of entries in this index */
  @volatile
  protected var _entries: Int = mmap.position() / entrySize

  /**
   * True iff there are no more slots available in this index
   */
  def isFull: Boolean = _entries >= _maxEntries

  def file: File = _file

  def maxEntries: Int = _maxEntries

  def entries: Int = _entries

  def length: Long = _length

  def updateParentDir(parentDir: File): Unit = _file = new File(parentDir, file.getName)

  /**
   * Reset the size of the memory map and the underneath file. This is used in two kinds of cases: (1) in
   * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
   * loading segments from disk or truncating back to an old segment where a new log segment became active;
   * we want to reset the index size to maximum index size to avoid rolling new segment.
   *
   * @param newSize new size of the index file
   * @return a boolean indicating whether the size of the memory map and the underneath file is changed or not.
   */
  def resize(newSize: Int): Boolean = {
    inLock(lock) {
      val roundedNewSize = roundDownToExactMultiple(newSize, entrySize)

      if (_length == roundedNewSize) {
        debug(s"Index ${file.getAbsolutePath} was not resized because it already has size $roundedNewSize")
        false
      } else {
        val raf = new RandomAccessFile(file, "rw")
        try {
          val position = mmap.position()

          /* Windows or z/OS won't let us modify the file length while the file is mmapped :-( */
          if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS)
            safeForceUnmap()
          raf.setLength(roundedNewSize)
          _length = roundedNewSize
          mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize)
          _maxEntries = mmap.limit() / entrySize
          mmap.position(position)
          debug(s"Resized ${file.getAbsolutePath} to $roundedNewSize, position is ${mmap.position()} " +
            s"and limit is ${mmap.limit()}")
          true
        } finally {
          CoreUtils.swallow(raf.close(), AbstractIndex)
        }
      }
    }
  }

  /**
   * Rename the file that backs this offset index
   *
   * @throws IOException if rename fails
   */
  def renameTo(f: File): Unit = {
    try Utils.atomicMoveWithFallback(file.toPath, f.toPath)
    finally _file = f
  }

  /**
   * Flush the data in the index to disk
   */
  def flush(): Unit = {
    inLock(lock) {
      mmap.force()
    }
  }

  /**
   * Delete this index file.
   *
   * @throws IOException if deletion fails due to an I/O error
   * @return `true` if the file was deleted by this method; `false` if the file could not be deleted because it did
   *         not exist
   */
  def deleteIfExists(): Boolean = {
    closeHandler()
    Files.deleteIfExists(file.toPath)
  }

  /**
   * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from
   * the file.
   */
  def trimToValidSize(): Unit = {
    inLock(lock) {
      resize(entrySize * _entries)
    }
  }

  /**
   * The number of bytes actually used by this index
   */
  def sizeInBytes: Int = entrySize * _entries

  /** Close the index */
  def close(): Unit = {
    trimToValidSize()
    closeHandler()
  }

  def closeHandler(): Unit = {
    // On JVM, a memory mapping is typically unmapped by garbage collector.
    // However, in some cases it can pause application threads(STW) for a long moment reading metadata from a physical disk.
    // To prevent this, we forcefully cleanup memory mapping within proper execution which never affects API responsiveness.
    // See https://issues.apache.org/jira/browse/KAFKA-4614 for the details.
    inLock(lock) {
      safeForceUnmap()
    }
  }

  /**
   * Do a basic sanity check on this index to detect obvious problems
   *
   * @throws CorruptIndexException if any problems are found
   */
  def sanityCheck(): Unit

  /**
   * Remove all the entries from the index.
   */
  protected def truncate(): Unit

  /**
   * Remove all entries from the index which have an offset greater than or equal to the given offset.
   * Truncating to an offset larger than the largest in the index has no effect.
   */
  def truncateTo(offset: Long): Unit

  /**
   * Remove all the entries from the index and resize the index to the max index size.
   */
  def reset(): Unit = {
    truncate()
    resize(maxIndexSize)
  }

  /**
   * Get offset relative to base offset of this index
   * @throws IndexOffsetOverflowException
   */
  def relativeOffset(offset: Long): Int = {
    val relativeOffset = toRelative(offset)
    if (relativeOffset.isEmpty)
      throw new IndexOffsetOverflowException(s"Integer overflow for offset: $offset (${file.getAbsoluteFile})")
    relativeOffset.get
  }

  /**
   * Check if a particular offset is valid to be appended to this index.
   * @param offset The offset to check
   * @return true if this offset is valid to be appended to this index; false otherwise
   */
  def canAppendOffset(offset: Long): Boolean = {
    toRelative(offset).isDefined
  }

  protected def safeForceUnmap(): Unit = {
    if (mmap != null) {
      try forceUnmap()
      catch {
        case t: Throwable => error(s"Error unmapping index $file", t)
      }
    }
  }

  /**
   * Forcefully free the buffer's mmap.
   */
  protected[log] def forceUnmap(): Unit = {
    try ByteBufferUnmapper.unmap(file.getAbsolutePath, mmap)
    finally mmap = null // Accessing unmapped mmap crashes JVM by SEGV so we null it out to be safe
  }

  /**
   * Execute the given function in a lock only if we are running on windows or z/OS. We do this
   * because Windows or z/OS won't let us resize a file while it is mmapped. As a result we have to force unmap it
   * and this requires synchronizing reads.
   */
  protected def maybeLock[T](lock: Lock)(fun: => T): T = {
    if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS)
      lock.lock()
    try fun
    finally {
      if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS)
        lock.unlock()
    }
  }

  /**
   * To parse an entry in the index.
   *
   * @param buffer the buffer of this memory mapped index.
   * @param n the slot
   * @return the index entry stored in the given slot.
   */
  protected def parseEntry(buffer: ByteBuffer, n: Int): IndexEntry

  /**
   * Find the slot in which the largest entry less than or equal to the given target key or value is stored.
   * The comparison is made using the `IndexEntry.compareTo()` method.
   *
   * @param idx The index buffer
   * @param target The index key to look for
   * @return The slot found or -1 if the least entry in the index is larger than the target key or the index is empty
   */
  protected def largestLowerBoundSlotFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchEntity): Int =
    indexSlotRangeFor(idx, target, searchEntity)._1

  /**
   * Find the smallest entry greater than or equal the target key or value. If none can be found, -1 is returned.
   */
  protected def smallestUpperBoundSlotFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchEntity): Int =
    indexSlotRangeFor(idx, target, searchEntity)._2

  /**
   * Lookup lower and upper bounds for the given target.
   */
  private def indexSlotRangeFor(idx: ByteBuffer, target: Long, searchEntity: IndexSearchEntity): (Int, Int) = {
    // check if the index is empty
    if(_entries == 0)
      return (-1, -1)

    def binarySearch(begin: Int, end: Int) : (Int, Int) = {
      // binary search for the entry
      var lo = begin
      var hi = end
      while(lo < hi) {
        val mid = (lo + hi + 1) >>> 1
        val found = parseEntry(idx, mid)
        val compareResult = compareIndexEntry(found, target, searchEntity)
        if(compareResult > 0)
          hi = mid - 1
        else if(compareResult < 0)
          lo = mid
        else
          return (mid, mid)
      }
      (lo, if (lo == _entries - 1) -1 else lo + 1)
    }

    val firstHotEntry = Math.max(0, _entries - 1 - _warmEntries)
    // check if the target offset is in the warm section of the index
    if(compareIndexEntry(parseEntry(idx, firstHotEntry), target, searchEntity) < 0) {
      return binarySearch(firstHotEntry, _entries - 1)
    }

    // check if the target offset is smaller than the least offset
    if(compareIndexEntry(parseEntry(idx, 0), target, searchEntity) > 0)
      return (-1, 0)

    binarySearch(0, firstHotEntry)
  }

  private def compareIndexEntry(indexEntry: IndexEntry, target: Long, searchEntity: IndexSearchEntity): Int = {
    searchEntity match {
      case IndexSearchType.KEY => java.lang.Long.compare(indexEntry.indexKey, target)
      case IndexSearchType.VALUE => java.lang.Long.compare(indexEntry.indexValue, target)
    }
  }

  /**
   * Round a number to the greatest exact multiple of the given factor less than the given number.
   * E.g. roundDownToExactMultiple(67, 8) == 64
   */
  private def roundDownToExactMultiple(number: Int, factor: Int) = factor * (number / factor)

  private def toRelative(offset: Long): Option[Int] = {
    val relativeOffset = offset - baseOffset
    if (relativeOffset < 0 || relativeOffset > Int.MaxValue)
      None
    else
      Some(relativeOffset.toInt)
  }

}

object AbstractIndex extends Logging {
  override val loggerName: String = classOf[AbstractIndex].getName
}

object IndexSearchType extends Enumeration {
  type IndexSearchEntity = Value
  val KEY, VALUE = Value
}
