package de.oneandone.eventtracker.redis.trie

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import scala.util.Random

class QueryTest extends FunSuite with BeforeAndAfter {
  val trie = new DistributedTrie
  trie.addNode("127.0.0.1", 6379)
  val random = new Random()
  
  before {
    trie.clearAll()
  }
  
  after {
    trie.clearAll()
  }
  
  test("Querying an existing or non-existing path") {
    // Generate random bytes and store their path.
    val numBytes = 8
    var bytes = new Array[Byte](numBytes)
    random.nextBytes(bytes)
    trie.setPath(bytes)
    // Flip a bit.
    val bitIndex = random.nextInt(numBytes * 8)
    bytes(bitIndex >> 3) = (bytes(bitIndex >> 3) ^ (1 << (8 - (bitIndex & (8 - 1)) - 1))).toByte
    // Query new path.
    if (bitIndex == 0)
      assert(trie.getLongestPath(bytes) === None)
    else
      assert(trie.getLongestPath(bytes) === Some(bitIndex - 1))
  }
  
  
  test("Querying an existing prefix") {
    // Generate random bytes and store their path.
    val numBytes = 8
    var bytes = new Array[Byte](numBytes)
    random.nextBytes(bytes)
    val prefixIndex = random.nextInt(numBytes * 8)
    trie.setPrefix(bytes, prefixIndex)
    // Query prefix.
    assert(trie.getLongestPrefix(bytes) === Some(prefixIndex))
  }
  
  
  test("Querying a non-existing prefix") {
    // Generate random bytes and store their path.
    val numBytes = 8
    var bytes = new Array[Byte](numBytes)
    random.nextBytes(bytes)
    val prefixIndex = random.nextInt(numBytes * 8)
    trie.setPrefix(bytes, prefixIndex)
    // Flip first bit.
    val bitIndex = 0
    bytes(bitIndex >> 3) = (bytes(bitIndex >> 3) ^ (1 << (8 - (bitIndex & (8 - 1)) - 1))).toByte
    // Query prefix.
    assert(trie.getLongestPrefix(bytes) === None)
  }
}