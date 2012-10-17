package de.oneandone.eventtracker.redis.trie

import org.scalatest.FunSuite

class LSBTest extends FunSuite {
  
  private def getLSB1(x: Int): Int = {
    return LSBTest.MultiplyDeBruijnBitPosition(((x & -x) * 0x077CB531) >>> 27)
  }
  
  private def getLSB2(x: Int): Int = {
    var value = x
    if (value == 0)
      return 0
    var pos = 0;
	while ((value & 1) == 0) {
      value >>>= 1
      pos += 1
	}
	return pos;
  }
  
  test("Least Significant Bit") {
    for (i <- 0 to 0xFFFFFFFF)
      assert(getLSB1(i) === (getLSB2(i)))
  }
}


object LSBTest {
  private val MultiplyDeBruijnBitPosition = Array(
    0, 1, 28, 2, 29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4, 8, 
    31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6, 11, 5, 10, 9)
}