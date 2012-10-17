package de.oneandone.eventtracker.redis.trie

import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import org.apache.commons.pool.impl.GenericObjectPool

class RedisNode(host: String, port: Int) {
  val poolConfig = new JedisPoolConfig()
  poolConfig.setMaxActive(200)
  poolConfig.setMaxIdle(50)
  val connections: JedisPool = new JedisPool(poolConfig, host, port)
  
  /**
   * Update an existing bitmap stored on this node (as a hash value) by merging it with a new one.
   * The update is done in a transaction and retried until the transaction succeeds.
   * @param key		Key of the hash which corresponds to the current bitmap.
   * @param field	Field inside the hash.
   * @param	toMerge	New bitmap to be merged with.
   * @return 		The merged bitmap. 
   */
  def mergeBitmap(key: Array[Byte], field: Array[Byte], toMerge: Bitmap): Bitmap = {
    val client = connections.getResource()
    try {
      client.watch(key)
      val bytes = client.hget(key, field)
      val merged = if (bytes == null) toMerge else new Bitmap(bytes.length * 8, bytes).union(toMerge)
      var result: java.util.List[Object] = null
      do {
        val t = client.multi()
        t.hset(key, field, merged.getBytes())
        result = t.exec()
      } while (result == null)
      return merged
    } finally {
      connections.returnResource(client)
    }
  }
  
  
  def getBitmap(key: Array[Byte], field: Array[Byte]): Option[Bitmap] = {
    val client = connections.getResource()
    try {
      val bytes = client.hget(key, field)
      return if (bytes != null) Some(new Bitmap(bytes.length * 8, bytes)) else None
    } finally {
      connections.returnResource(client)
    }
  }
  
  
  def flushAll() = {
    val client = connections.getResource()
    try {
      client.flushAll()
    } finally {
      connections.returnResource(client)
    }
  }
}