package de.oneandone.eventtracker.infinispanhotrod.trie
import scala.collection.mutable.ArrayBuffer

object KeysEncoder {
  
  /**
   * @param bytes			The array of bytes to query.
   * @param strideLength	The length in bits of each stride that the byte array will be split into.
   * 						(Must be a multiple of 8.)
   * @return				The corresponding keys for each stride.
   */
  def getStridesKeys(bytes: Array[Byte], strideLength: Int): Array[Array[Byte]] = {
    if (bytes.isEmpty) return Array.empty
    
    val strideLengthBytes = strideLength / 8
    val keys = new ArrayBuffer[Array[Byte]](bytes.length / strideLengthBytes)
    var end = 0
    
    while (end < bytes.length) {
      val lastBit = ((bytes(end) >> 7) & 1).toByte
      val prefix = bytes.slice(0, end) :+ lastBit
      keys += prefix
      end += strideLengthBytes
    }
    return keys.toArray
  }
  
  
  def debugKey(key: Array[Byte]): Unit = {
    key.foreach(c => print("0x%02X " format c))
    println()
  }
}