package de.oneandone.eventtracker.CRDT.ORset;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

/**
 * A sparse matrix-like representation of timestamps.
 * @author adeftu
 *
 */
public class Timestamps implements Serializable {
	private static final long serialVersionUID = -9160347112338408557L;
	private HashMap<String, HashMap<String, Long>> timestamps = new HashMap<String, HashMap<String, Long>>();
	
	/**
	 * Get the timestamp of a store.
	 * @param rc	Replica cluster ID.
	 * @param rs	Replica store ID.
	 * @return		Timestamp value.
	 */
	public Long get(String rc, String rs) {
		HashMap<String, Long> cluster = timestamps.get(rc);
		if (cluster == null)
			return 0L;
		Long t = cluster.get(rs);
		if (t == null)
			return 0L;
		return t;
	}
	
	
	/**
	 * Set the timestamp of a store.
	 * @param rc	Replica cluster ID.
	 * @param rs	Replica store ID.
	 * @param t		Timestamp value.
	 */
	public void set(String rc, String rs, Long t) {
		HashMap<String, Long> cluster = timestamps.get(rc);
		if (cluster == null) {
			HashMap<String, Long> newCluster = new HashMap<String, Long>();
			timestamps.put(rc, newCluster);
			cluster = newCluster;
		}
		cluster.put(rs, t);
	}
	
	
	/**
	 * Get all cluster IDs.
	 */
	public Set<String> getClusterIDs() {
		return timestamps.keySet();
	}
	
	
	/**
	 * Get all store IDs from the given cluster ID.
	 */
	public Set<String> getStoreIDs(String rc) {
		return timestamps.get(rc).keySet();
	}
	
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (String rc : timestamps.keySet()) {
			for (String rs : timestamps.get(rc).keySet()) {
				sb.append(String.format("%s:%s: %d\n", rc, rs, timestamps.get(rc).get(rs)));
			}
		}
		return sb.toString();
	}
}
