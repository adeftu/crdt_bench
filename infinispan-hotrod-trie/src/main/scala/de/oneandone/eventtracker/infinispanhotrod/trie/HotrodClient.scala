package de.oneandone.eventtracker.infinispanhotrod.trie

import org.infinispan.client.hotrod.RemoteCache
import org.infinispan.client.hotrod.RemoteCacheManager
import java.util.Properties
import scala.collection.mutable.ArrayBuffer

class HotrodClient {
  private val servers = new ArrayBuffer[(String, Int)]()
  private var cacheManager: RemoteCacheManager = null
  private var prefixCache: RemoteCache[Array[Byte], Array[Byte]] = null
  private var pathCache: RemoteCache[Array[Byte], Array[Byte]] = null
  
  def addServer(host: String, port: Int) = {
    servers += new Tuple2(host, port)
  }
  
  def start(prefixCacheName: String, pathCacheName: String) = {
    val properties = new Properties
    val serverList = servers.foldLeft("")((list, server) => list + server._1 + ":" + server._2 + ";")
    properties.put("infinispan.client.hotrod.server_list", serverList)
    properties.put("infinispan.client.hotrod.hash_function_impl.2", "org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV2")
    properties.put("infinispan.client.hotrod.ping_on_startup", "false")
    cacheManager = new RemoteCacheManager(properties)
    prefixCache = cacheManager.getCache(prefixCacheName)
    pathCache = cacheManager.getCache(pathCacheName)
  }
  
  def clearAll() = {
    prefixCache.clear()
    pathCache.clear()
  }
  
  def stop() = {
    cacheManager.stop()
  }
  
  /**
   * Update an existing bitmap (as a hash value) by merging it with a new one.
   * @param key         Key of the hash which corresponds to the current bitmap.
   * @param cacheName   The name of the cache.
   * @param toMerge     New bitmap to be merged with.
   * @return            The merged bitmap. 
   */
  def mergeBitmap(key: Array[Byte], cacheName: String, toMerge: Bitmap): Bitmap = {
    val cache = cacheName match {
      case DistributedTrie.BITMAP_PREFIX_CACHE => prefixCache
      case DistributedTrie.BITMAP_PATH_CACHE => pathCache
    }
    val bytes = cache.get(key)
    val merged = if (bytes == null) toMerge else new Bitmap(bytes.length * 8, bytes).union(toMerge)
    cache.put(key, merged.getBytes())
    return merged
  }
  
  
  def getBitmap(key: Array[Byte], cacheName: String): Option[Bitmap] = {
    val cache = cacheName match {
      case DistributedTrie.BITMAP_PREFIX_CACHE => prefixCache
      case DistributedTrie.BITMAP_PATH_CACHE => pathCache
    }
    val bytes = cache.get(key)
    return if (bytes != null) Some(new Bitmap(bytes.length * 8, bytes)) else None
  }
}