package de.oneandone.eventtracker.infinispanhotrod.trie

import java.io.FileInputStream
import java.io.FileNotFoundException
import java.util.Properties

import scala.Array.canBuildFrom
import scala.util.Random

import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite

case object Concurrency extends Enumeration {
  type Concurrency = Value
  val SingleThread, ForkJoin, ResizableThreadPool = Value
}

class Bench extends FunSuite with BeforeAndAfter {
  val trie = new DistributedTrie
  val random = new Random()
  
  before {
    
  }
  
  after {
    
  }
  
  test("Random 'set' and 'get' performance test") {
    val props = new Properties()
    
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
       
    // PROPS_FILE_NAME
    try {
      props.load(new FileInputStream(Bench.PROPS_FILE_NAME))
    } catch {
      case e: FileNotFoundException => { fail("Properties file %s not found!" format Bench.PROPS_FILE_NAME) }
    }
    
    // PROPS_SERVERS
    val servers:String = props.getProperty(Bench.PROPS_SERVERS)
    servers match {
      case s:String => servers.split(";").foreach(server => { val pair = server.split(":"); trie.addServer(pair(0), pair(1).toInt); infoProperty("server", pair) })
      case null => { trie.addServer("127.0.0.1", 11222); infoProperty("server", ("127.0.0.1", 11222)) }
    }
    
    val querySize    = readIntProperty(Bench.PROPS_QUERY_SIZE, 64)
    val strideSize   = readIntProperty(Bench.PROPS_STRIDE_SIZE, 8)
    trie.setStrideLength(strideSize)
    val setPathOps   = readIntProperty(Bench.PROPS_SET_PATH_OPS, 1000)
    val setPrefixOps = readIntProperty(Bench.PROPS_SET_PREFIX_OPS, 1000)
    val getPathOps   = readIntProperty(Bench.PROPS_GET_PATH_OPS, 1000)
    val getPrefixOps = readIntProperty(Bench.PROPS_GET_PREFIX_OPS, 1000)
    
    // PROPS_CONCURRENCY
    var concurrency = Concurrency.ForkJoin
    val c = props.getProperty(Bench.PROPS_CONCURRENCY)
    if (c != null) {
      if (c == "single-thread") concurrency = Concurrency.SingleThread
      else if (c == "fork-join") concurrency = Concurrency.ForkJoin
      else if (c == "resizable-thread-pool") concurrency = Concurrency.ResizableThreadPool      
    }
    infoProperty(Bench.PROPS_CONCURRENCY, concurrency)
          
    // Set path queries.
    val setPathQueries = new Array[Array[Byte]](setPathOps)
    for (i <- 0 until setPathQueries.length) {
      setPathQueries(i) = new Array[Byte](querySize / 8)
      random.nextBytes(setPathQueries(i))
    }
    
    // Set prefix queries.
    val setPrefixQueries = new Array[(Array[Byte], Int)](setPrefixOps)
    for (i <- 0 until setPrefixQueries.length) {
      setPrefixQueries(i) = (new Array[Byte](querySize / 8), random.nextInt(querySize))
      random.nextBytes(setPrefixQueries(i)._1)
    }
    
    // Get path queries.
    val getPathQueries = new Array[Array[Byte]](getPathOps)
    for (i <- 0 until getPathQueries.length) {
      getPathQueries(i) = new Array[Byte](querySize / 8)
      random.nextBytes(getPathQueries(i))
    }
    
    // Get prefix queries.
    val getPrefixQueries = new Array[Array[Byte]](getPrefixOps)
    for (i <- 0 until getPrefixQueries.length) {
      getPrefixQueries(i) = new Array[Byte](querySize / 8)
      random.nextBytes(getPrefixQueries(i))
    }
    
    // Run benchmark.
    trie.start()
    trie.clearAll()
    
    val startTime = System.nanoTime
    
    if (concurrency == Concurrency.SingleThread) {
      setPathQueries.foreach(q => trie.setPath(q))
      setPrefixQueries.foreach(q => trie.setPrefix(q._1, q._2))
      getPathQueries.foreach(q => trie.getLongestPath(q))
      getPrefixQueries.foreach(q => trie.getLongestPrefix(q))
    }
    else {
      if (concurrency == Concurrency.ForkJoin)
        System.setProperty("actors.enableForkJoin", "true")
      else {
        System.setProperty("actors.enableForkJoin", "false")
      }
      
      val tasks = setPathQueries.map(q => scala.actors.Futures.future { trie.setPath(q) }) ++ 
                  setPrefixQueries.map(q => scala.actors.Futures.future { trie.setPrefix(q._1, q._2) }) ++
                  getPathQueries.map(q => scala.actors.Futures.future { trie.getLongestPath(q) }) ++
                  getPrefixQueries.map(q => scala.actors.Futures.future { trie.getLongestPrefix(q) })
      tasks.foreach(_())
    }
    
    val elapsedMillis = (System.nanoTime - startTime)/1000000.0
    info("Elapsed time: %.2f (ms)" format elapsedMillis)
    
    trie.clearAll()
    trie.stop()
  }
}

object Bench {
  val PROPS_FILE_NAME      = "bench.properties"
  val PROPS_SERVERS        = "servers"
  val PROPS_QUERY_SIZE     = "query_size"
  val PROPS_STRIDE_SIZE    = "stride_size"
  val PROPS_SET_PATH_OPS   = "set_path_ops"
  val PROPS_SET_PREFIX_OPS = "set_prefix_ops"
  val PROPS_GET_PATH_OPS   = "get_path_ops"
  val PROPS_GET_PREFIX_OPS = "get_prefix_ops"
  val PROPS_CONCURRENCY    = "concurrency"
}