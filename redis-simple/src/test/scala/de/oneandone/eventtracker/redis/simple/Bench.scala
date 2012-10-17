package de.oneandone.eventtracker.redis.simple

import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import java.io.FileNotFoundException
import java.util.Properties
import java.io.FileInputStream
import redis.clients.jedis.JedisPool
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

case object Concurrency extends Enumeration {
  type Concurrency = Value
  val SingleThread, ForkJoin, ResizableThreadPool = Value
}

class Bench extends FunSuite with BeforeAndAfter {
  test("Redis simple benchmark") {
    val props = new Properties()
    val servers: ArrayBuffer[JedisPool] = new ArrayBuffer()
    val random = new Random()
    
    def infoProperty(key: String, value: Any) = {
      info("Using %s = %s" format (key, 
        value match {
          case s:String => s
          case a:Array[String] => a.mkString(":")
          case (s:String, i:Int) => "%s:%d" format (s, i)
          case a:Any => a.toString()
        }
      ))
    }
    
    def readIntProperty(property: String, default: Int): Int = {
      val value = if (props.getProperty(property) != null) props.getProperty(property).toInt else default
      infoProperty(property, value)
      return value
    }
    
   
    def addServer(host: String, port: Int) = {
      servers += new JedisPool(host, port)
      infoProperty("server", (host, port))
    }
    
    
    def selectServer(key: Array[Byte]) : JedisPool = {
      val hash = key.foldLeft(0) { (total, byte) => 31 * total + byte }
      return servers((hash % servers.length).abs)
    }
    
    
    // PROPS_FILE_NAME
    try {
      props.load(new FileInputStream(Bench.PROPS_FILE_NAME))
    } catch {
      case e: FileNotFoundException => { fail("Properties file %s not found!" format Bench.PROPS_FILE_NAME) }
    }
    
    
    // PROPS_SERVERS
    val str:String = props.getProperty(Bench.PROPS_SERVERS)
    str match {
      case s:String => str.split(";").foreach(server => { val pair = server.split(":"); addServer(pair(0), pair(1).toInt) })
      case null => addServer("127.0.0.1", 6379)
    }
     
    
    // PROPS_CONCURRENCY
    var concurrency = Concurrency.SingleThread
    val c = props.getProperty(Bench.PROPS_CONCURRENCY)
    if (c != null) {
      if (c == "single-thread") concurrency = Concurrency.SingleThread
      else if (c == "fork-join") { concurrency = Concurrency.ForkJoin; System.setProperty("actors.enableForkJoin", "true"); }
      else if (c == "resizable-thread-pool") { concurrency = Concurrency.ResizableThreadPool; System.setProperty("actors.enableForkJoin", "false"); }      
    }
    infoProperty(Bench.PROPS_CONCURRENCY, concurrency)
    
    val stringSetCount  = readIntProperty(Bench.PROPS_STRING_SET_COUNT, 1000)
    val stringGetCount  = readIntProperty(Bench.PROPS_STRING_GET_COUNT, 1000)
    val stringKeySize   = readIntProperty(Bench.PROPS_STRING_KEY_SIZE, 8)
    val stringValueSize = readIntProperty(Bench.PROPS_STRING_VALUE_SIZE, 32)
    val hashHSetCount   = readIntProperty(Bench.PROPS_HASH_HSET_COUNT, 1000)
    val hashHGetCount   = readIntProperty(Bench.PROPS_HASH_HGET_COUNT, 1000)
    val hashFieldCount  = readIntProperty(Bench.PROPS_HASH_FIELD_COUNT, 100)
    val hashKeySize     = readIntProperty(Bench.PROPS_HASH_KEY_SIZE, 8)
    val hashFieldSize   = readIntProperty(Bench.PROPS_HASH_FIELD_SIZE, 8)
    val hashValueSize   = readIntProperty(Bench.PROPS_HASH_VALUE_SIZE, 32)
    

    def flushAll() = {
      servers.foreach(server => { 
        val client = server.getResource();
        client.flushAll();
        server.returnResource(client);
      })
    }
    
    flushAll()
    
    /**************************
     ******** STRINGS *********
     **************************/
    
    // SET
    def set() = {
      val key = new Array[Byte](stringKeySize)
      random.nextBytes(key)
      val value = new Array[Byte](stringValueSize)
      random.nextBytes(value)
      val server = selectServer(key)
      val client = server.getResource()
      client.set(key, value)
      server.returnResource(client)
    }
    
    val startTimeSet = System.nanoTime 
    if (concurrency == Concurrency.SingleThread) {
      for (i <- 0 until stringSetCount) set()
    }
    else {
      val setTasks = new ArrayBuffer[scala.actors.Future[Any]](stringSetCount)
      for (i <- 0 until stringSetCount) setTasks += scala.actors.Futures.future { set() }
      setTasks.foreach(_())
    }
    info("[SET] Elapsed time: %.2f (ms)" format (System.nanoTime - startTimeSet)/1000000.0)

    
    // GET
    def get() = {
      val key = new Array[Byte](stringKeySize)
      random.nextBytes(key)
      val server = selectServer(key)
      val client = server.getResource()
      client.get(key)
      server.returnResource(client)
    }
    
    val startTimeGet = System.nanoTime
    if (concurrency == Concurrency.SingleThread) {
      for (i <- 0 until stringGetCount) get()
    }
    else {
      val getTasks = new ArrayBuffer[scala.actors.Future[Any]](stringGetCount)
      for (i <- 0 until stringGetCount) getTasks += scala.actors.Futures.future { get() }
      getTasks.foreach(_())
    }
    info("[GET] Elapsed time: %.2f (ms)" format (System.nanoTime - startTimeGet)/1000000.0)
    
    
    /**************************
     ********* HASHES *********
     **************************/

    // HSET
    def hset() = {
      val key = new Array[Byte](hashKeySize)
      random.nextBytes(key)
      val field = new Array[Byte](hashFieldSize)
      random.nextBytes(field)
      val value = new Array[Byte](hashValueSize)
      random.nextBytes(value)
      val server = selectServer(key)
      val client = server.getResource()
      client.hset(key, field, value)
      server.returnResource(client)
    }
    
    val startTimeHSet = System.nanoTime 
    if (concurrency == Concurrency.SingleThread) {
      for (i <- 0 until hashHSetCount) for (j <- 0 until hashFieldCount) hset()
    }
    else {
      val hsetTasks = new ArrayBuffer[scala.actors.Future[Any]](hashHSetCount * hashFieldCount)
      for (i <- 0 until hashHSetCount) for (j <- 0 until hashFieldCount) hsetTasks += scala.actors.Futures.future { hset() }
      hsetTasks.foreach(_())
    }
    info("[HSET] Elapsed time: %.2f (ms)" format (System.nanoTime - startTimeHSet)/1000000.0)
    

    // HGET
    def hget() = {
      val key = new Array[Byte](hashKeySize)
      random.nextBytes(key)
      val field = new Array[Byte](hashFieldSize)
      random.nextBytes(field)
      val server = selectServer(key)
      val client = server.getResource()
      client.hget(key, field)
      server.returnResource(client)
    }
    
    val startTimeHGet = System.nanoTime 
    if (concurrency == Concurrency.SingleThread) {
      for (i <- 0 until hashHGetCount) for (j <- 0 until hashFieldCount) hget()
    }
    else {
      val hgetTasks = new ArrayBuffer[scala.actors.Future[Any]](hashHGetCount * hashFieldCount)
      for (i <- 0 until hashHGetCount) for (j <- 0 until hashFieldCount) hgetTasks += scala.actors.Futures.future { hget() }
      hgetTasks.foreach(_())
    }
    info("[HGET] Elapsed time: %.2f (ms)" format (System.nanoTime - startTimeHGet)/1000000.0)
    
    flushAll()
  }
}


object Bench {
  val PROPS_FILE_NAME           = "bench.properties"
  val PROPS_SERVERS             = "servers"
  val PROPS_STRING_SET_COUNT    = "string_set_count"
  val PROPS_STRING_GET_COUNT    = "string_get_count"
  val PROPS_STRING_KEY_SIZE     = "string_key_size"
  val PROPS_STRING_VALUE_SIZE   = "string_value_size"
  val PROPS_HASH_HSET_COUNT     = "hash_hset_count"
  val PROPS_HASH_HGET_COUNT     = "hash_hget_count"
  val PROPS_HASH_FIELD_COUNT    = "hash_field_count"
  val PROPS_HASH_KEY_SIZE       = "hash_key_size"
  val PROPS_HASH_FIELD_SIZE     = "hash_field_size"
  val PROPS_HASH_VALUE_SIZE     = "hash_value_size"
  val PROPS_CONCURRENCY         = "concurrency"
}