package de.oneandone.eventtracker.infinispanhotrod.simple

import org.infinispan.client.hotrod.RemoteCache
import org.infinispan.client.hotrod.RemoteCacheManager
import java.util.Properties
import scala.collection.mutable.ArrayBuffer

class HotrodClient {
  private val servers = new ArrayBuffer[(String, Int)]()
  private var cacheManager: RemoteCacheManager = null
  private var cache: RemoteCache[Array[Byte], Array[Byte]] = null
  
  def addServer(host: String, port: Int) = {
    servers += new Tuple2(host, port)
  }
  
  def start() = {
    val properties = new Properties
    val serverList = servers.foldLeft("")((list, server) => list + server._1 + ":" + server._2 + ";")
    properties.put("infinispan.client.hotrod.server_list", serverList)
    properties.put("infinispan.client.hotrod.hash_function_impl.2", "org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV2")
    properties.put("infinispan.client.hotrod.ping_on_startup", "false")
	cacheManager = new RemoteCacheManager(properties)
    cache = cacheManager.getCache()
  }
  
  def put(k: Array[Byte], v: Array[Byte]) = {
    cache.put(k, v)
  }
  
  def get(k: Array[Byte]): Array[Byte] = {
    return cache.get(k)
  }
  
  def clear() = {
    cache.clear();
  }
  
  def stop() = {
    cacheManager.stop()
  }
}