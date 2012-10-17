package de.oneandone.eventtracker.infinispanhotrod.trie

class DistributedTrie {
  val client = new HotrodClient
  var strideLength = 8
  
  /**
   * Set the stride length in bits. Must be a multiple of 8. The default one is 8.
   */
  def setStrideLength(stride: Int) = {
    require(stride > 0 && stride % 8 == 0)
    strideLength = stride
  }
  
  /**
   * Add a new server to the cache cluster.
   */
  def addServer(host: String, port: Int) = {
    client.addServer(host, port)
  }
  
  def start() = {
    client.start(DistributedTrie.BITMAP_PREFIX_CACHE, DistributedTrie.BITMAP_PATH_CACHE)
  }
  
  def stop() = {
    client.stop()
  }
  
  
  /**
   * Creates a new bitmap from a stride representation.
   * @param bytes           Stride to convert.
   * @param prefixBitIndex  Index of the last bit for the prefix to store in the bitmap
   *                        (must be between 0 and bytes.length*8-1). 
   *                        If set to 'None', then prefixes for all bits are stored.
   */
  private def newBitmapFromStride(bytes: Array[Byte], prefixBitIndex: Option[Int]): Bitmap = {
    require(bytes.length * 8 == strideLength)
    
    val bitmap = new Bitmap(1 << strideLength)
    
    var index = 0
    if (prefixBitIndex == None)
      bitmap.setBit(index)
    for (
      i <- 1 to (prefixBitIndex match {
        case Some(value) => { require(value < strideLength); value }
        case None => strideLength - 1
      })
    ) {
      val bit = (bytes(i >> 3) >> (8 - (i & (8 - 1)) - 1)) & 1
      if (bit == 0)
        index = (index << 1) + 1
      else
        index = (index << 1) + 2
      if (prefixBitIndex == None)
        bitmap.setBit(index)
    }
    if (prefixBitIndex != None)
      bitmap.setBit(index)

    return bitmap
  }
  
  
  /**
   * Get the stride index of the last common bit between two bitmaps that is set.
   */
  private def getStrideLastCommonBit(a: Bitmap, b: Bitmap): Option[Int] = {
    val bit = a.getLastCommonBit(b)
    if (bit == None) {
      return None
    }
    else {
      var bitStrideIndex = 0        // Index between 0..(strideLength - 1)
      var bitTrieIndex = bit.get    // Index between 0..(((1 << strideLength) - 1) == Bitmap.size)
      while (bitTrieIndex != 0) {
        bitTrieIndex = (bitTrieIndex - 1) / 2
        bitStrideIndex += 1
      }
      return Some(bitStrideIndex)
    }
  }
  
  
  /**
   * Store all prefixes (the path) of a bytes sequence.
   * @param bytes   Sequence of bytes for which to store prefixes.
   */
  def setPath(bytes: Array[Byte]) = {
    val keys = KeysEncoder.getStridesKeys(bytes, strideLength)
    val toMergeBitmaps = bytes.grouped(strideLength / 8).toList.map(stride => newBitmapFromStride(stride, None))
    val mergedBitmaps = (keys, toMergeBitmaps).zipped map (client.mergeBitmap(_, DistributedTrie.BITMAP_PATH_CACHE, _))
  }
  
  
  /**
   * Store the prefix ending at a given bit index in a bytes sequence.
   * @param bytes   Sequence of bytes for which to store the prefix.
   * @param lastBit Index of the last bit for the prefix (must be between 0 and bytes.length * 8).
   */
  def setPrefix(bytes: Array[Byte], lastBit: Int) = {
    require(lastBit < bytes.length * 8)
    val strideIndex = lastBit / strideLength   
    val key = KeysEncoder.getStridesKeys(bytes, strideLength)(strideIndex)
    val toMergeBitmap = newBitmapFromStride(bytes.grouped(strideLength / 8).toList(strideIndex), Some(lastBit % strideLength))
    val mergedBitmap = client.mergeBitmap(key, DistributedTrie.BITMAP_PREFIX_CACHE, toMergeBitmap)
  }
  
  
  /**
   * Search for the last common bit between each query bitmap and each stored bitmap.
   * Do this in reverse order and fetch only a subgroup of stored bitmaps to avoid unnecessary
   * network overhead. Stop when the first matching bit that is set in both bitmaps is found.
   */
  private def getLastCommonBit(bytes: Array[Byte], cacheName: String): Option[Int] = {
    val queryBitmaps = bytes.grouped(strideLength / 8).toList.map(stride => newBitmapFromStride(stride, None))
    val keys = KeysEncoder.getStridesKeys(bytes, strideLength)
    assert(queryBitmaps.length == keys.length)
    var bitmapIndex = queryBitmaps.length
    (keys.reverse.grouped(keys.length / 2).toList, queryBitmaps.reverse.grouped(queryBitmaps.length / 2).toList).zipped.
      foreach((keysGroup, queryBitmapsGroup) => {
        val storedBitmapsGroup = keysGroup map (client.getBitmap(_, cacheName))
        (queryBitmapsGroup, storedBitmapsGroup).zipped.
          foreach((queryBitmap, storedBitmap) => {
            bitmapIndex -= 1
            if (storedBitmap != None) {
              val commonBit = getStrideLastCommonBit(queryBitmap, storedBitmap.get)
              if (commonBit != None)
                return Some(bitmapIndex * strideLength + commonBit.get)
            }
          })
    })
    return None
  }
  
  
  /**
   * Query a bytes sequence for the longest stored path.
   * @return    The index of the last common bit between the bytes sequence and any stored path 
   *            or 'None' if no common path was found.
   */
  def getLongestPath(bytes: Array[Byte]): Option[Int] = {
    return getLastCommonBit(bytes, DistributedTrie.BITMAP_PATH_CACHE)
  }
  
  
  /**
   * Query a bytes sequence for the longest stored prefix.
   * @return     The index of the last common bit between the bytes sequence and any stored prefix
   *             or 'None' if no common prefix was found.
   */
  def getLongestPrefix(bytes: Array[Byte]): Option[Int] = {
    return getLastCommonBit(bytes, DistributedTrie.BITMAP_PREFIX_CACHE)
  }
  
  
  /**
   * Clear cache.
   */
  def clearAll() = {
    client.clearAll()
  }
}

object DistributedTrie {
  val BITMAP_PREFIX_CACHE = "prefix-cache"
  val BITMAP_PATH_CACHE   = "path-cache"
}