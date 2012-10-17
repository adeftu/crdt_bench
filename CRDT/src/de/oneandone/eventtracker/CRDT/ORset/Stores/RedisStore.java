package de.oneandone.eventtracker.CRDT.ORset.Stores;

import java.io.File;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import de.oneandone.eventtracker.CRDT.ORset.Element;
import de.oneandone.eventtracker.CRDT.ORset.Store;
import de.oneandone.eventtracker.CRDT.ORset.Timestamps;
import de.oneandone.eventtracker.CRDT.ORset.Topology;

public class RedisStore extends Store {
	private JedisPool jedisPool;

	private static final String KEY_TOPOLOGY = "topology";
	
	private static String SCRIPT_SHA1_ADD;
	private static String SCRIPT_SHA1_REMOVE;
	private static String SCRIPT_SHA1_LOOKUP;
	private static String SCRIPT_SHA1_GET_TIMESTAMPS;
	private static String SCRIPT_SHA1_SET_MAX_TIMESTAMP;

	private void init() {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setTestOnBorrow(true);
		config.setTestOnReturn(true);
		config.setMaxWait(0);
		jedisPool = new JedisPool(config, address.getHostName(), address.getPort(), 0);
		loadScripts();
		setOnline(true);
	}
	
	public RedisStore(InetSocketAddress address) {
		super(address);
		init();
	}
	
	public RedisStore(String rc, String rs, InetSocketAddress address) {
		super(rc, rs, address);
		init();
	}

	private String loadRedisScript(String fileName) throws Exception {
		FileInputStream fis = null;
		Jedis jedis = jedisPool.getResource();
		try {
			File file = new File(fileName);
			byte[] buffer = new byte[(int) file.length()];
			fis = new FileInputStream(file);
			fis.read(buffer);
			return jedis.scriptLoad(new String(buffer, "us-ascii"));
		} catch (Exception e) {
			throw e;
		}
		finally {
			if (fis != null)
				fis.close();
			jedisPool.returnResource(jedis);
		}
	}
	
	private void loadScripts() {
		try {
			SCRIPT_SHA1_ADD = loadRedisScript("lua/orset/add.lua");
			SCRIPT_SHA1_REMOVE = loadRedisScript("lua/orset/remove.lua");
			SCRIPT_SHA1_LOOKUP = loadRedisScript("lua/orset/lookup.lua");
			SCRIPT_SHA1_GET_TIMESTAMPS = loadRedisScript("lua/orset/get_timestamps.lua");
			SCRIPT_SHA1_SET_MAX_TIMESTAMP = loadRedisScript("lua/orset/set_max_timestamp.lua");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void clearAndSetTopology(String fileName) throws Exception {
		Topology topology = new Topology();
		topology.loadFromFile(fileName);
		
		for (String rc : topology.getClusterIDs()) {
			for (String rs : topology.getStoreIDs(rc)) {
				InetSocketAddress address = topology.get(rc, rs);
				Jedis jedis = new Jedis(address.getHostName(), address.getPort());
				jedis.flushAll();
				jedis.scriptFlush();
				for (String rc_ : topology.getClusterIDs()) {
					for (String rs_ : topology.getStoreIDs(rc_)) {
						InetSocketAddress address_ = topology.get(rc_, rs_);
						jedis.sadd(KEY_TOPOLOGY, String.format("%s:%s:%s:%s", rc_, rs_, address_.getHostName(), address_.getPort()));
					}
				}
				jedis.disconnect();
			}
		}
	}
	
	/**
	 * Read the topology from redis. This is to be found at key 'topology'
	 * containing a list of nodes, each in the format rc:rs:ip:port.
	 */
	@Override
	public Topology getTopology() throws TimeoutException {
		if (!isOnline())
			throw new TimeoutException("Timeout while fetching the topology");
		
		Topology topology = new Topology();
		Jedis jedis = jedisPool.getResource();
		try {
			Set<String> nodes = jedis.smembers(KEY_TOPOLOGY);
			for (String node : nodes) {
				String[] tokens = node.split(":");
				String rc = tokens[0];
				String rs = tokens[1];
				String ip = tokens[2];
				String port = tokens[3];
				topology.set(rc, rs, new InetSocketAddress(ip, Integer.parseInt(port)));
			}
			return topology;
		} finally {
			jedisPool.returnResource(jedis);
		}
	}

	@Override
	public void setOnline(boolean online) {
		super.setOnline(online);
		Jedis jedis = jedisPool.getResource();
		try {
			jedis.set("online", String.valueOf(online));
		} finally {
			jedisPool.returnResource(jedis);
		}
	}
	
	public boolean isOnline() {
		if (!checkIfOnline)
			return true;
		Jedis jedis = jedisPool.getResource();
		try {
			return Boolean.valueOf(jedis.get("online"));
		} finally {
			jedisPool.returnResource(jedis);
		}
	}
	
	
	@Override
	public void add(String value) throws TimeoutException, IllegalArgumentException {
		if (value.contains(":"))
			throw new IllegalArgumentException("Values are not allowed to contain ':' character");
		if (!isOnline())
			throw new TimeoutException(String.format("Timeout while adding %s", value));
		
		Jedis jedis = jedisPool.getResource();
		try {
			jedis.evalsha(SCRIPT_SHA1_ADD, 0, String.valueOf(Element.getTTL()), value, rc, rs);
		} finally {
			jedisPool.returnResource(jedis);
		}
	}

	
	@Override
	public void remove(String value) throws TimeoutException {
		if (value.contains(":"))
			throw new IllegalArgumentException("Values are not allowed to contain ':' character");
		if (!isOnline())
			throw new TimeoutException(String.format("Timeout while removing %s", value));
		
		Jedis jedis = jedisPool.getResource();
		try {
			jedis.evalsha(SCRIPT_SHA1_REMOVE, 0, String.valueOf(Element.getTTL()), value, rc, rs);
		} finally {
			jedisPool.returnResource(jedis);
		}
	}

	
	@Override
	public boolean lookup(String value) throws TimeoutException {
		if (value.contains(":"))
			throw new IllegalArgumentException("Values are not allowed to contain ':' character");
		if (!isOnline())
			throw new TimeoutException(String.format("Timeout while looking up %s", value));
		
		Jedis jedis = jedisPool.getResource();
		try {
			Object result = jedis.evalsha(SCRIPT_SHA1_LOOKUP, 0, value);
			if (result == null)
				return false;
			else
				return true;
		} finally {
			jedisPool.returnResource(jedis);
		}
	}

	@Override
	public Timestamps getTimestamps() throws TimeoutException {
		if (!isOnline())
			throw new TimeoutException("Timeout while getting timestamps");
		
		Jedis jedis = jedisPool.getResource();
		try {
			@SuppressWarnings("unchecked")
			ArrayList<String> result = (ArrayList<String>) jedis.evalsha(SCRIPT_SHA1_GET_TIMESTAMPS);	// result = [rc:rs:t, ...]
			Timestamps timestamps = new Timestamps();
			for (String a : result) {
				String[] tokens = a.split(":");
				timestamps.set(tokens[0], tokens[1], Long.valueOf(tokens[2]));
			}
			return timestamps;
		} finally {
			jedisPool.returnResource(jedis);
		}
	}

	@Override
	public void updateMaxTimestamps(Timestamps other) throws TimeoutException {
		if (!isOnline())
			throw new TimeoutException("Timeout while updating timestamps");
		
		// TODO
		// Call scripts in pipeline when there is support in Jedis.
		Jedis jedis = jedisPool.getResource();
		try {
			for (String rc : other.getClusterIDs()) {
				for (String rs : other.getStoreIDs(rc)) {
					jedis.evalsha(SCRIPT_SHA1_SET_MAX_TIMESTAMP, 0, rc, rs, other.get(rc, rs).toString());
				}
			}
		} finally {
			jedisPool.returnResource(jedis);
		}
	}

	
	@Override
	public List<Element> getUpdates(Timestamps timestamps) throws TimeoutException {
		if (!isOnline())
			throw new TimeoutException("Timeout while getting updates");
		
		updateStats.getUpdateIDs = updateStats.getUpdateElements = 0;
		long startTime;
				
		Jedis jedis = jedisPool.getResource();
		try {
			LinkedList<Element> updates = new LinkedList<Element>();
			for (String rc : timestamps.getClusterIDs()) {
				for (String rs : timestamps.getStoreIDs(rc)) {
					final String key = String.format("index:%s:%s", rc, rs);
					final long listLength = jedis.llen(key);
					final long PAGE_SIZE = Math.min(10000, listLength); // Set the page size to the number of expected updates.
					int timestampIndex = -1;
					for (long crtPageStart = -listLength; crtPageStart < 0 && timestampIndex < 0; crtPageStart += PAGE_SIZE) {
						// Get update IDs in fixed-sized pages.
						startTime = System.nanoTime();
						List<String> page = jedis.lrange(key, crtPageStart, crtPageStart + PAGE_SIZE - 1);
						updateStats.getUpdateIDs += (System.nanoTime() - startTime) / 1000000.0;
						timestampIndex = Collections.binarySearch(page, String.format("%d:", timestamps.get(rc, rs)),
								new Comparator<String>() {
									@Override
									public int compare(String o1, String o2) {
										final long t1 = Long.parseLong(o1.substring(0, o1.indexOf(':')));
										final long t2 = Long.parseLong(o2.substring(0, o2.indexOf(':')));
										if (t1 > t2) return -1;
										if (t1 < t2) return 1;
										return 0;
									}
								});
						
						// For each update ID in the page, fetch the corresponding tuple. Do this in a pipeline.
						Pipeline p = jedis.pipelined();
						int crtPageElementIndex = 0;
						ArrayList<String> ids = new ArrayList<String>(page.size());
						for (Iterator<String> it = page.iterator(); it.hasNext() && crtPageElementIndex < (timestampIndex < 0 ? page.size() : timestampIndex); ++crtPageElementIndex) {
							final String crtPageElement = it.next();
							final String id = crtPageElement.substring(crtPageElement.indexOf(':') + 1);
							startTime = System.nanoTime();
							p.hmget(String.format("element:%s", id), "value", "added.t", "added.rc", "added.rs", "removed.t", "removed.rc", "removed.rs");
							p.ttl(String.format("element:%s", id));
							updateStats.getUpdateElements += (System.nanoTime() - startTime) / 1000000.0;
							ids.add(id);
						}
						// Flush pipeline and append tuples to the list of updates.
						startTime = System.nanoTime();
						ArrayList<Object> result = (ArrayList<Object>) p.syncAndReturnAll();
						updateStats.getUpdateElements += (System.nanoTime() - startTime) / 1000000.0;
						Iterator<String> idsIt = ids.iterator();
						for (int i = 0; i < result.size(); ) {
							@SuppressWarnings("unchecked")
							final ArrayList<String> tuple = (ArrayList<String>) result.get(i++);
							final long ttl = (Long) result.get(i++);
							final String id = idsIt.next();
							if (tuple.get(1) != null) {				// Key is not expired
								updates.addFirst(new Element(tuple.get(0), 
										Long.parseLong(tuple.get(1)), tuple.get(2), tuple.get(3), 
										tuple.get(4) != null ? Long.parseLong(tuple.get(4)) : null, tuple.get(5), tuple.get(6), 
										(int) ttl, id));
							}
						}
					}
				}
			}
			return updates;
		} finally {
			jedisPool.returnResource(jedis);
		}
	}

	@Override
	public void addUpdates(List<Element> updates) throws TimeoutException {
		if (!isOnline())
			throw new TimeoutException("Timeout while adding updates");
		
		updateStats.addUpdateElements = 0;
		long startTime;

		Jedis jedis = jedisPool.getResource();
		try {
			if (updates == null)
				return;

			// Add update elements to the store. Do this in a pipeline.
			// TODO In case one update fails to be added, it should be retried and inserted s.t. index:rc:rs will be still sorted.
			Pipeline p = jedis.pipelined();
			for (Element e : updates) {
				HashMap<String, String> tuple = new HashMap<String, String>();
				tuple.put("value", e.value);
				tuple.put("added.t", e.added_t.toString());
				tuple.put("added.rc", e.added_rc);
				tuple.put("added.rs", e.added_rs);
				if (e.removed_t != null) {
					tuple.put("removed.t", e.removed_t.toString());
					tuple.put("removed.rc", e.removed_rc);
					tuple.put("removed.rs", e.removed_rs);
				}
				startTime = System.nanoTime();
				p.multi();
				p.hmset(String.format("element:%s", e.id), tuple);
				if (e.removed_t != null)
					p.lpush(String.format("index:%s:%s", e.removed_rc, e.removed_rs), String.format("%d:%s", e.removed_t, e.id));
				else
					p.lpush(String.format("index:%s:%s", e.added_rc, e.added_rs), String.format("%d:%s", e.added_t, e.id));
				if (e.gc_ttl >= 0)
					p.expire(String.format("element:%s", e.id), e.gc_ttl);
				p.sadd(String.format("ids:%s", e.value), e.id);
				p.exec();
				updateStats.addUpdateElements += (System.nanoTime() - startTime) / 1000000.0;
			}
			startTime = System.nanoTime();
			p.sync();
			updateStats.addUpdateElements += (System.nanoTime() - startTime) / 1000000.0;
		} finally {
			jedisPool.returnResource(jedis);
		}
	}

	@Override
	public void clear() throws TimeoutException {
		Jedis jedis = jedisPool.getResource();
		try {
			jedis.flushAll();
			jedis.scriptFlush();
		} finally {
			jedisPool.returnResource(jedis);
		}
	}

	@Override
	public void close() {
		jedisPool.destroy();
	}

//	// For debugging only.
//	@Override
//	public String toString() {
//		try {
//			boolean online = isOnline();
//			setOnline(true);
//			StringBuilder sb = new StringBuilder();
//			sb.append(String.format("** %s:%s **\n", rc, rs));
//			sb.append(String.format("> online: %b\n", online));
//			
//			Timestamps timestamps = getTimestamps();
//			Topology topology = getTopology();
//			for (String rc : topology.getClusterIDs()) {
//				for (String rs : topology.getStoreIDs(rc)) {
//					timestamps.set(rc, rs, Math.max(timestamps.get(rc, rs), 0));
//				}
//			}
//			sb.append(String.format("> timestamps:\n%s", timestamps));
//			
//			sb.append("> elements:\n");
//			for (String rc : topology.getClusterIDs()) {
//				for (String rs : topology.getStoreIDs(rc)) {
//					timestamps.set(rc, rs, 0L);
//				}
//			}
//			for (Element e : getUpdates(timestamps))
//				sb.append(String.format("%s\n", e));
//			setOnline(online);
//			return sb.toString();
//		} catch (Exception e) {
//			e.printStackTrace();
//			return null;
//		}
//	}
}
