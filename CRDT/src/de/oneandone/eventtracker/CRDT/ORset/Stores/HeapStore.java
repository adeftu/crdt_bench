package de.oneandone.eventtracker.CRDT.ORset.Stores;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import de.oneandone.eventtracker.CRDT.ORset.Element;
import de.oneandone.eventtracker.CRDT.ORset.Store;
import de.oneandone.eventtracker.CRDT.ORset.Timestamps;
import de.oneandone.eventtracker.CRDT.ORset.Topology;


public class HeapStore extends Store {

	private static Topology topology;
	private String DB_FILE = "db_";
	
	private LinkedList<Element> elements = new LinkedList<Element>();
	private Timestamps timestamps = new Timestamps();
	
	public HeapStore(InetSocketAddress address) {
		super(address);
	}

	
	public HeapStore(String rc, String rs, InetSocketAddress address) {
		super(rc, rs, address);
		DB_FILE += String.format("%s:%s.bin", rc, rs);
	}

	@SuppressWarnings("unchecked")
	public void readDB() {
		if (!new File(DB_FILE).exists()) {
			timestamps = new Timestamps();
			elements = new LinkedList<Element>();
			return;
		}
		try {
			ObjectInputStream in = new ObjectInputStream(new FileInputStream(DB_FILE));
			online = in.readBoolean();
			timestamps = (Timestamps) in.readObject();
			elements = (LinkedList<Element>) in.readObject();
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void writeDB() {
		try {
			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(DB_FILE));
			out.writeBoolean(online);
			out.writeObject(timestamps);
			out.writeObject(elements);
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void clear() throws TimeoutException {
		new File(DB_FILE).delete();
		timestamps = new Timestamps();
		elements = new LinkedList<Element>();
	}
	
	public static void clearAndSetTopology(String fileName) throws Exception {
		topology = new Topology();
		topology.loadFromFile(fileName);
		for (String rc : topology.getClusterIDs()) {
			for (String rs : topology.getStoreIDs(rc)) {
				new HeapStore(rc, rs, new InetSocketAddress("", 0)).clear();
			}
		}
	}
	
	private boolean isOnline() {
		if (!checkIfOnline)
			return true;
		return online;
	}
	
	/**
	 * Return a mockup topology from a configuration file defined in TOPOLOGY_CONFIG_FILE. No IPs are used.
	 */
	@Override
	public Topology getTopology() throws Exception {
		readDB();
		if (!isOnline())
			throw new TimeoutException("Timeout while fetching the topology");
		return topology;
	}

	@Override
	public void setOnline(boolean online) {
		super.setOnline(online);
		writeDB();
	}
	
	
	@Override
	public void add(String value) throws TimeoutException {
		readDB();
		if (!isOnline())
			throw new TimeoutException(String.format("Timeout while adding %s", value));
		Long t = timestamps.get(rc, rs) + 1;
		timestamps.set(rc, rs, t);
		Element e = new Element(value, t, rc, rs, null, null, null);
		e.gc_time = System.currentTimeMillis();
		elements.add(e);
		writeDB();
	}

	private boolean isExpired(Element e) {
		if (Element.getTTL() < 0)
			return false;
		return e.gc_time + Element.getTTL() * 1000 <= System.currentTimeMillis();
	}
	
	@Override
	public void remove(String value) throws TimeoutException {
		readDB();
		if (!isOnline())
			throw new TimeoutException(String.format("Timeout while removing %s", value));
		Long t = timestamps.get(rc, rs) + 1;
		timestamps.set(rc, rs, t);
		for (Element e : elements) {
			if (!isExpired(e) && e.value.equals(value)) {
				e.removed_t = t;
				e.removed_rc = rc;
				e.removed_rs = rs;
				e.gc_time = System.currentTimeMillis();
			}
		}
		writeDB();
	}

	@Override
	public boolean lookup(String value) throws TimeoutException {
		readDB();
		if (!isOnline())
			throw new TimeoutException(String.format("Timeout while looking up %s", value));
		for (Element e : elements) {
			if (!isExpired(e) &&
				e.value.equals(value) && 
				e.removed_t == null) {
				boolean exists = true;
				for (Element f : elements) {
					if (!isExpired(f) &&
						f.value.equals(value) &&
						f.added_t.longValue() == e.added_t.longValue() &&
						f.added_rc.equals(e.added_rc) &&
						f.added_rs.equals(e.added_rs) &&
						f.removed_t != null)
						exists = false;
				}
				if (exists)
					return true;
			}
		}
		return false;
	}

	@Override
	public Timestamps getTimestamps() throws TimeoutException {
		readDB();
		if (!isOnline())
			throw new TimeoutException("Timeout while getting timestamps");
		return timestamps;
	}

	@Override
	public void updateMaxTimestamps(Timestamps other) throws TimeoutException {
		readDB();
		if (!isOnline())
			throw new TimeoutException("Timeout while updating timestamps");
		for (String rc : other.getClusterIDs()) {
			for (String rs : other.getStoreIDs(rc)) {
				timestamps.set(rc, rs, Math.max(timestamps.get(rc, rs), other.get(rc, rs)));
			}
		}
		writeDB();
	}

	@Override
	public List<Element> getUpdates(Timestamps timestamps) throws TimeoutException {
		readDB();
		if (!isOnline())
			throw new TimeoutException("Timeout while getting updates");
		LinkedList<Element> updates = new LinkedList<Element>();
		for (Element e : elements) {
			if (!isExpired(e)) {
				if (e.removed_t == null) {
					if (timestamps.get(e.added_rc, e.added_rs).longValue() < e.added_t.longValue())
						updates.add(e);
				}
				else if (timestamps.get(e.removed_rc, e.removed_rs).longValue() < e.removed_t.longValue())
					updates.add(e);
			}
		}
		return updates;
	}

	@Override
	public void addUpdates(List<Element> updates) throws TimeoutException {
		readDB();
		if (!isOnline())
			throw new TimeoutException("Timeout while adding updates");
		if (updates == null)
			return;
		elements.addAll(updates);
		writeDB();
	}

//	// For debugging only.
//	@Override
//	public String toString() {
//		readDB();
//		StringBuilder sb = new StringBuilder();
//		sb.append(String.format("** %s:%s **\n", rc, rs));
//		sb.append(String.format("> online: %b\n", online));
//		sb.append(String.format("> timestamps:\n%s", timestamps));
//		sb.append("> elements:\n");
//		for (Element e : elements)
//			sb.append(String.format("%s\n", e));
//		return sb.toString();
//	}


	@Override
	public void close() {
		
	}
}
