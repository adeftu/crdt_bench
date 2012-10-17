package de.oneandone.eventtracker.infinispanhotrod.simple

import java.io.FileInputStream
import java.io.FileNotFoundException
import java.util.Properties
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

case object Concurrency extends Enumeration {
  type Concurrency = Value
  val SingleThread, ForkJoin, ResizableThreadPool = Value
}

class Bench extends FunSuite with BeforeAndAfter {
  
  before {
  }
  
  after {
  }
  
  
  test("Infinispan simple benchmark") {
    val client = new HotrodClient()
    val props = new Properties()
    val random = new Random()
    
    def infoProperty(key: String, value: Any) = {
      info("Using %s = %s" format (key, 
        value match {
          case s:String => s
          case a:Array[String] => a.mkString(":")
          case a:Any => a.toString()
        }
      ))
    }
    
    def readIntProperty(property: String, default: Int): Int = {
      val value = if (props.getProperty(property) != null) props.getProperty(property).toInt else default
      infoProperty(property, value)
      return value
    }
    
    // PROPS_FILE_NAME
    try {
      props.load(new FileInputStream(Bench.PROPS_FILE_NAME))
    } catch {
      case e: FileNotFoundException => { fail("Properties file %s not found!" format Bench.PROPS_FILE_NAME) }
    }
    
    // PROPS_SERVERS
    val servers = props.getProperty(Bench.PROPS_SERVERS)
    servers match {
      case s:String => servers.split(";").foreach(server => { val pair = server.split(":"); client.addServer(pair(0), pair(1).toInt); infoProperty("server", pair) })
      case null => client.addServer("127.0.0.1", 11222)
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
    
    val putOps    = readIntProperty(Bench.PROPS_PUT_OPS, 10000)
    val getOps    = readIntProperty(Bench.PROPS_GET_OPS, 10000)
    val keySize   = readIntProperty(Bench.PROPS_KEY_SIZE, 8)
    val valueSize = readIntProperty(Bench.PROPS_VALUE_SIZE, 32)
    
    client.start()
    client.clear()
    
    
    // PUT
    def put() = {
      val key = new Array[Byte](keySize)
      random.nextBytes(key)
      val value = new Array[Byte](valueSize)
      random.nextBytes(value)
      client.put(key, value)
    }
    
    val startTimePut = System.nanoTime
    if (concurrency == Concurrency.SingleThread) {
      for (i <- 0 until putOps) put()
    }
    else {
      val putTasks = new ArrayBuffer[scala.actors.Future[Any]](putOps)
      for (i <- 0 until putOps) putTasks += scala.actors.Futures.future { put() }
      putTasks.foreach(_())
    }
    info("[PUT] Elapsed time: %.2f (ms)" format (System.nanoTime - startTimePut)/1000000.0)
    

    // GET
    def get() = {
      val key = new Array[Byte](keySize)
      random.nextBytes(key)
      client.get(key)
    }
    
    val startTimeGet = System.nanoTime
    if (concurrency == Concurrency.SingleThread) {
      for (i <- 0 until getOps) get()
    }
    else {
      val getTasks = new ArrayBuffer[scala.actors.Future[Any]](getOps)
      for (i <- 0 until getOps) getTasks += scala.actors.Futures.future { get() }
      getTasks.foreach(_())
    }
    info("[GET] Elapsed time: %.2f (ms)" format (System.nanoTime - startTimeGet)/1000000.0)
    
    
    client.clear()
    client.stop()
  }
}


object Bench {
  val PROPS_FILE_NAME      = "bench.properties"
  val PROPS_SERVERS        = "servers"
  val PROPS_PUT_OPS        = "put_ops"
  val PROPS_GET_OPS        = "get_ops"
  val PROPS_KEY_SIZE       = "key_size"
  val PROPS_VALUE_SIZE     = "value_size"
  val PROPS_CONCURRENCY    = "concurrency"
}