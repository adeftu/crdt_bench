package de.oneandone.eventtracker.redis.simple

/**
 * Generates strings of given length.
 */
class StringGenerator(length: Int) {
  var next: Int = 0
  var prev: Int = 0
  
  def intToString(a: Int): String = {
    val md = java.security.MessageDigest.getInstance("SHA-1")
    val ha = new sun.misc.BASE64Encoder().encode(md.digest(a.toString().getBytes()))
    if (length <= ha.length())
      return ha.substring(0, length)
    else {
      val str = new StringBuilder()
      var added = 0
      while (added != length) { 
        val toAdd = if (added + ha.length() <= length) ha.length else length - added
        str.append(ha.substring(0, toAdd))
        added += toAdd
      }
      return str.toString()
    }
  }
  
  /**
   * Get a new string and add it to the store.
   */
  def getNext(): String = {
    next += 1
    intToString(next)
  }
  
  
  /**
   * Get a previously generated string and remove it from the store.
   */
  def getPrev(): Option[String] = {
    if (prev < next) {
      prev += 1
      Some(intToString(prev))
    }
    else
      None
  }
}