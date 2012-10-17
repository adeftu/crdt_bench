package de.oneandone.eventtracker.redis.trie

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import scala.util.Random
import redis.clients.jedis.JedisPool
import scala.actors.Actor

class TransactionTest extends FunSuite with BeforeAndAfter {
  val connections: JedisPool = new JedisPool("127.0.0.1", 6379)

  def flushAll() = {
    val client = connections.getResource()
    try {
      client.flushAll()
    } finally {
      connections.returnResource(client)
    }
  }
  
  before {
    flushAll()
  }
  
  after {
    flushAll()
  }
  
  test("Redis transactions") {
    val key   = "TransactionTest"
    val field = "TransactionTest"
    var actor1SetCount = 0
    var actor2SetCount = 0
    
    class Actor1 extends Actor {
	  def act {
	    val client = connections.getResource()
	    try {
	      client.watch(key)
	      val oldValue = client.hget(key, field)
	      Thread.sleep(2000)
	      val newValue = "Actor1"
	      var result: java.util.List[Object] = null
	      do {
	        val t = client.multi()
	        t.hset(key, field, newValue)
	        actor1SetCount += 1
	        result = t.exec()
	      } while (result == null)
	    } finally {
	      connections.returnResource(client)
	    }
	  }
	}
    
    class Actor2 extends Actor {
	  def act {
	    val client = connections.getResource()
	    try {
	      client.watch(key)
	      val oldValue = client.hget(key, field)
	      Thread.sleep(1000)
	      val newValue = "Actor2"
	      var result: java.util.List[Object] = null
	      do {
	        val t = client.multi()
	        t.hset(key, field, newValue)
	        actor2SetCount += 1
	        result = t.exec()
	      } while (result == null)
	    } finally {
	      connections.returnResource(client)
	    }
	  }
	}
    
    var actors = List[Actor](new Actor1, new Actor2)
    val tasks = actors map (actor => scala.actors.Futures.future { actor.act() })
    tasks.foreach(_())
    
    assert(actor1SetCount === 2)
    assert(actor2SetCount === 1)
  }
}