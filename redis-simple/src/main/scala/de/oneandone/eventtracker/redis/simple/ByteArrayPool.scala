package de.oneandone.eventtracker.redis.simple

import scala.collection.mutable.Queue
import scala.util.Random

class ByteArrayPool(size: Int, byteArraySize: Int) {
  val next = new Queue[Array[Byte]]()
  val prev = new Queue[Array[Byte]]()

  val random = new Random()
  (1 to size) foreach (_ => { val array = new Array[Byte](byteArraySize); random.nextBytes(array); next += array} )
  
  def getNext(): Option[Array[Byte]] = {
    if (next.size > 0) {
      val v = next.dequeue()
      prev += v
      return Some(v)
    }
    else
      return None
  }
  
  def getNextAll(): Seq[Array[Byte]] = {
    val all = next.dequeueAll(_ => true)
    prev ++= all
    return all
  }
  
  def getPrev(): Option[Array[Byte]] = {
    if (prev.size > 0)
      return Some(prev.dequeue())
    else
      return None
  }
}