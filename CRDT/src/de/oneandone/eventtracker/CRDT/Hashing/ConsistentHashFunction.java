package de.oneandone.eventtracker.CRDT.Hashing;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashFunction<T> implements HashFunction<T> {
	private final SortedMap<Long, T> circle = new TreeMap<Long, T>();
	private static final int NUM_OF_REPLICAS = 3;
	private final MD5 md5 = new MD5();
	
	@Override
	public synchronized void add(T node) {
		for (int i = 0; i < NUM_OF_REPLICAS; i++)
			circle.put(md5.hash(node.toString() + i), node);
	}

	@Override
	public synchronized void add(Collection<T> nodes) {
		for (T node : nodes)
			add(node);
	}

	@Override
	public synchronized void remove(T node) {
		for (int i = 0; i < NUM_OF_REPLICAS; i++)
			circle.remove(md5.hash(node.toString() + i));
	}

	@Override
	public synchronized T get(String key) {
		if (circle.isEmpty()) {
			return null;
		}
		long hash = md5.hash(key);
		if (!circle.containsKey(hash)) {
			SortedMap<Long, T> tailMap = circle.tailMap(hash);
			hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
		}
		return circle.get(hash);
	}
}
