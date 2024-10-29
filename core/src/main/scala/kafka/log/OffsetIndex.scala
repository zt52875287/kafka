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

import java.io.File
import java.nio.ByteBuffer

import kafka.utils.CoreUtils.inLock
import kafka.utils.Logging
import org.apache.kafka.common.errors.InvalidOffsetException

/**
 * An index that maps offsets to physical file locations for a particular log segment. This index may be sparse:
 * that is it may not hold an entry for all messages in the log.
 *
 * The index is stored in a file that is pre-allocated to hold a fixed maximum number of 8-byte entries.
 *
 * The index supports lookups against a memory-map of this file. These lookups are done using a simple binary search variant
 * to locate the offset/location pair for the greatest offset less than or equal to the target offset.
 *
 * Index files can be opened in two ways: either as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 *
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 *
 * The file format is a series of entries. The physical format is a 4 byte "relative" offset and a 4 byte file location for the
 * message with that offset. The offset stored is relative to the base offset of the index file. So, for example,
 * if the base offset was 50, then the offset 55 would be stored as 5. Using relative offsets in this way let's us use
 * only 4 bytes for the offset.
 *
 * The frequency of entries is up to the user of this class.
 *
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal
 * storage format.
 */
// Avoid shadowing mutable `file` in AbstractIndex
class OffsetIndex(_file: File, baseOffset: Long, maxIndexSize: Int = -1, writable: Boolean = true)
    extends AbstractIndex(_file, baseOffset, maxIndexSize, writable) {
  import OffsetIndex._

  override def entrySize = 8

  /* the last offset in the index */
  private[this] var _lastOffset = lastEntry.offset

  debug(s"Loaded index file ${file.getAbsolutePath} with maxEntries = $maxEntries, " +
    s"maxIndexSize = $maxIndexSize, entries = ${_entries}, lastOffset = ${_lastOffset}, file position = ${mmap.position()}")

  /**
   * The last entry in the index
   */
  private def lastEntry: OffsetPosition = {
    inLock(lock) {
      _entries match {
        case 0 => OffsetPosition(baseOffset, 0)
        case s => parseEntry(mmap, s - 1)
      }
    }
  }

  def lastOffset: Long = _lastOffset

  /**
   * Find the largest offset less than or equal to the given targetOffset
   * and return a pair holding this offset and its corresponding physical file position.
   *
   * 在索引文件中查找 offset 刚好小于等于 targetOffset 的那一个
   *
   * @param targetOffset The offset to look up.
   *                     相对位置
   * @return The offset found and the corresponding file position for this offset
   *         If the target offset is smaller than the least entry in the index (or the index is empty),
   *         the pair (baseOffset, 0) is returned.
   *         返回索引对应的 item 的 absolute offset 以及 physical position
   */
  def lookup(targetOffset: Long): OffsetPosition = {
    maybeLock(lock) {
      val idx = mmap.duplicate

      // 在索引文件中查找 offset 刚好小于等于 targetOffset 的那一个
      // （targetOffset 就是处于这个索引对应的数据块中）
      // slot 是指索引在 offsetIndex 中的第几条
      val slot = largestLowerBoundSlotFor(idx, targetOffset, IndexSearchType.KEY)

      if (slot == -1)
      // 如果没找到，则说明 targetOffset 比索引文件中所有的都小，则返回(索引文件的起始 offset, 起始 physical position=0)
        OffsetPosition(baseOffset, 0)
      else {
        // 如果确定 targetOffset 在当前索引所对应的 log 中，则返回索引中存储的信息
        // 1. 当前索引对应的 item 的绝对 offset
        // 2. 当前索引对应的 item 在 log 文件中的物理位置
        parseEntry(idx, slot)
      }
    }
  }

  /**
   * Find an upper bound offset for the given fetch starting position and size. This is an offset which
   * is guaranteed to be outside the fetched range, but note that it will not generally be the smallest
   * such offset.
   */
  def fetchUpperBoundOffset(fetchOffset: OffsetPosition, fetchSize: Int): Option[OffsetPosition] = {
    maybeLock(lock) {
      val idx = mmap.duplicate
      val slot = smallestUpperBoundSlotFor(idx, fetchOffset.position + fetchSize, IndexSearchType.VALUE)
      if (slot == -1)
        None
      else
        Some(parseEntry(idx, slot))
    }
  }

  private def relativeOffset(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * entrySize)

  // +4 是因为 offset 和 position 连着的，这里要跳过 offset
  private def physical(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * entrySize + 4)

  // 这里的 n 就是指，当前 offsetIndex 中的第 n 项（索引），里面存的是 item 的相对 offset值和物理位置
  override protected def parseEntry(buffer: ByteBuffer, n: Int): OffsetPosition = {
    // offset: 初始 offset 加上第 n 条 index 中保存的 offset 值
    OffsetPosition(baseOffset + relativeOffset(buffer, n), physical(buffer, n))
  }

  /**
   * Get the nth offset mapping from the index
   * @param n The entry number in the index
   * @return The offset/position pair at that entry
   */
  def entry(n: Int): OffsetPosition = {
    maybeLock(lock) {
      if (n >= _entries)
        throw new IllegalArgumentException(s"Attempt to fetch the ${n}th entry from index ${file.getAbsolutePath}, " +
          s"which has size ${_entries}.")
      parseEntry(mmap, n)
    }
  }

  /**
   * Append an entry for the given offset/location pair to the index. This entry must have a larger offset than all subsequent entries.
   * @throws IndexOffsetOverflowException if the offset causes index offset to overflow
   */
  def append(offset: Long, position: Int): Unit = {
    inLock(lock) {
      require(!isFull, "Attempt to append to a full index (size = " + _entries + ").")
      // 如果一条数据都没有，直接 put || 如果 offset 合法，则可以 put
      if (_entries == 0 || offset > _lastOffset) {
        trace(s"Adding index entry $offset => $position to ${file.getAbsolutePath}")

        // 这里是将（相对于整个分区的）绝对 offset，转变为（相对于当前 segment 的）相对 offset，方便后续查找
        mmap.putInt(relativeOffset(offset))
        mmap.putInt(position)
        _entries += 1
        _lastOffset = offset
        // 防御性编程，确保增加一个 entry(put 两个 Int)之后，mmap 的 position 是正确的
        require(_entries * entrySize == mmap.position(), s"$entries entries but file position in index is ${mmap.position()}.")
      } else {
        throw new InvalidOffsetException(s"Attempt to append an offset ($offset) to position $entries no larger than" +
          s" the last offset appended (${_lastOffset}) to ${file.getAbsolutePath}.")
      }
    }
  }

  override def truncate() = truncateToEntries(0)

  override def truncateTo(offset: Long): Unit = {
    inLock(lock) {
      val idx = mmap.duplicate
      val slot = largestLowerBoundSlotFor(idx, offset, IndexSearchType.KEY)

      /* There are 3 cases for choosing the new size
       * 1) if there is no entry in the index <= the offset, delete everything
       * 2) if there is an entry for this exact offset, delete it and everything larger than it
       * 3) if there is no entry for this offset, delete everything larger than the next smallest
       */
      val newEntries =
        if(slot < 0)
          0
        else if(relativeOffset(idx, slot) == offset - baseOffset)
          slot
        else
          slot + 1
      truncateToEntries(newEntries)
    }
  }

  /**
   * Truncates index to a known number of entries.
   */
  private def truncateToEntries(entries: Int): Unit = {
    inLock(lock) {
      _entries = entries
      mmap.position(_entries * entrySize)
      _lastOffset = lastEntry.offset
      debug(s"Truncated index ${file.getAbsolutePath} to $entries entries;" +
        s" position is now ${mmap.position()} and last offset is now ${_lastOffset}")
    }
  }

  override def sanityCheck(): Unit = {
    if (_entries != 0 && _lastOffset < baseOffset)
      throw new CorruptIndexException(s"Corrupt index found, index file (${file.getAbsolutePath}) has non-zero size " +
        s"but the last offset is ${_lastOffset} which is less than the base offset $baseOffset.")
    if (length % entrySize != 0)
      throw new CorruptIndexException(s"Index file ${file.getAbsolutePath} is corrupt, found $length bytes which is " +
        s"neither positive nor a multiple of $entrySize.")
  }

}

object OffsetIndex extends Logging {
  override val loggerName: String = classOf[OffsetIndex].getName
}
