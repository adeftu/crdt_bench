package de.oneandone.eventtracker.infinispanhotrod.trie


/**
 * @param size	Number of bits to store.
 */
class Bitmap(val size: Int) {
  /*
   *  Internal bitmap representation. Bits are stored in the 'bytes' array.
   *  Bit 0 is stored in ((bytes(0)) >> 7) & 1, bit 1 in ((bytes(0)) >> 6) & 1 and so on.
   */
  private var bytes = new Array[Byte](size >> 3)
  
  /**
   * Initialize bitmap from a sequence of bytes.
   */
  def this(size: Int, bytes: Array[Byte]) = {
    this(size)
    this.bytes = bytes
  }

  
  /**
   * Get the internal bytes used to store the bitmap.
   */
  def getBytes(): Array[Byte] = {
    return bytes
  }
  
  
  def debug(): Unit = {
    bytes.foreach(b => print("0x%02X " format b))
    println()
  }
  
  /**
   * Sets a bit at a given index (must be between 0 and 'size'-1).
   * Index 0 corresponds to first bit: (bytes(0)) >> 7 & 1.
   */
  def setBit(index: Int) = {
    require(index >= 0 && index < size)
    val byte = index >> 3
    val bit = (index & (8 - 1))
    bytes(byte) = (bytes(byte) | ((1 << (8 - bit - 1)) & 0xFF)).toByte
  }
  
  
  def getBit(index: Int): Boolean = {
    require(index >= 0 && index < size)
    val byte = index >> 3
    val bit = (index & (8 - 1))
    return if (((bytes(byte) >> (8 - bit - 1)) & 1) != 0) true else false
  }
  
  
  /**
   * Clear all bits.
   */
  def reset() = {
    bytes = Array.fill(bytes.length) { 0 }
  }
  
  
  /**
   * Do a bitwise OR between this bitmap and another one.
   */
  def union(that: Bitmap): Bitmap = {
    require(this.bytes.length == that.bytes.length)
    for (i <- 0 until this.bytes.length)
      this.bytes(i) = ((this.bytes(i) | that.bytes(i)) & 0xFF).toByte
    return this
  }
  
  
  /**
   * Get the position of the least significant bit that is set.
   */
  private def getLSB(x: Int): Int = {
    return Bitmap.MultiplyDeBruijnBitPosition(((x & -x) * 0x077CB531) >>> 27)
  }
  
  
  /**
   * Get the position of the last common bit between this bitmap and another one.
   */
  def getLastCommonBit(that: Bitmap): Option[Int] = {
    require(this.size == that.size)
    for (i <- bytes.length - 1 to 0 by -1) {
      val byte = this.bytes(i) & that.bytes(i)
      if (byte != 0)
        return Some(i * 8 + (8 - getLSB(byte) - 1))
    }
    return None
  }
}


object Bitmap {
  private val MultiplyDeBruijnBitPosition = Array(
    0, 1, 28, 2, 29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4, 8, 
    31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6, 11, 5, 10, 9)
}