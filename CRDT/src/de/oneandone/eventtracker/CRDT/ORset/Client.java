package de.oneandone.eventtracker.CRDT.ORset;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import de.oneandone.eventtracker.CRDT.Hashing.HashFunction;
import de.oneandone.eventtracker.CRDT.Hashing.ModuloHashFunction;

/**
 * Client class for accessing a replica cluster of an OR-set.
 * @author adeftu
 *
 * @param <T>	Type of DB store to use.
 */
public class Client<T extends Store> {
	private Class<T> clazz;
	private String rc = null;
	private HashMap<String, HashMap<String, T>> stores = new HashMap<String, HashMap<String, T>>();
	private HashFunction<T> hash = new ModuloHashFunction<T>();
	
	public Client(Class<T> clazz) {
		this.clazz = clazz;
	}
	
	public String getClusterID() {
		return rc;
	}

	public int getClusterSize() {
		return stores.get(rc).size();
	}
	
	
	/**
	 * Connect the client to the cluster through a bootstrap store.
	 * @param host	Host name or IP address of the bootstrap store.
	 * @param port	Port number of the bootstrap store on which the DB server is listening to.
	 */
	public void boot(String host, int port) throws RuntimeException {
		InetSocketAddress bootstrap = new InetSocketAddress(host, port);
		try {
			// Connect to bootstrap store and fetch topology.
			T bootStore = clazz.getConstructor(InetSocketAddress.class).newInstance(bootstrap);
			Topology topology = bootStore.getTopology();
			bootStore.close();
			// Create store clients for each store in topology.
			for (String rc : topology.getClusterIDs()) {
				HashMap<String, T> cluster = new HashMap<String, T>();
				for (String rs : topology.getStoreIDs(rc)) {
					InetSocketAddress address = topology.get(rc, rs);
					T store = clazz.getConstructor(String.class, String.class, InetSocketAddress.class).newInstance(rc, rs, address);
					cluster.put(rs, store);
					if (address.equals(bootstrap))
						this.rc = rc;
				}
				this.stores.put(rc, cluster);
				if (rc.equals(this.rc))
					hash.add(cluster.values());
			}
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
		if (this.rc == null)
			throw new RuntimeException(String.format("Bootstrap address %s not found in topology", bootstrap));
	}
	
	
	/**
	 * Get the corresponding store which hashes the value.
	 */
	public T getStore(String value) {
		return hash.get(value);
	}

	/**
	 * Set whether to check if the stores are online or not before any operation (for testing).
	 * @param checkIfStoresOnline
	 */
	public void setCheckIfStoresOnline(boolean checkIfStoresOnline) {
		for (HashMap<String, T> cluster : stores.values()) {
			for (T store : cluster.values()) {
				store.setCheckIfOnline(checkIfStoresOnline);
			}
		}
	}

	/**
	 * Add a value to the OR-set.
	 */
	public void add(String value) throws RuntimeException {
		T store = hash.get(value);
		if (store == null) {
			throw new RuntimeException("Tried to add a value through a non-booted client");
		}
		try {
			store.add(value);
		} catch (Exception e) {
			throw new RuntimeException(String.format("%s:%s: %s", store.getClusterID(), store.getStoreID(), e.getMessage()));
		}
	}
	

	/**
	 * Remove a value from the OR-set.
	 */
	public void remove(String value) throws RuntimeException {
		T store = hash.get(value);
		if (store == null) {
			throw new RuntimeException("Tried to remove a value through a non-booted client");
		}
		try {
			store.remove(value);
		} catch (Exception e) {
			throw new RuntimeException(String.format("%s:%s: %s", store.getClusterID(), store.getStoreID(), e.getMessage()));
		}
	}
	

	/**
	 * Search for a value in the OR-set.
	 */
	public boolean lookup(String value) throws RuntimeException {
		T store = hash.get(value);
		if (store == null) {
			throw new RuntimeException("Tried to lookup a value through a non-booted client");
		}
		try {
			return store.lookup(value);
		} catch (Exception e) {
			throw new RuntimeException(String.format("%s:%s: %s", store.getClusterID(), store.getStoreID(), e.getMessage()));
		}
	}

	/**
	 * Empty the DB of all stores in the current cluster.
	 */
	public void clear() throws RuntimeException {
		if (stores.get(rc) != null) {
			for (T store : stores.get(rc).values()) {
				try {
					store.clear();
				} catch (Exception e) {
					throw new RuntimeException(String.format("%s:%s: %s", store.getClusterID(), store.getStoreID(), e.getMessage()));
				}
			}
		}
	}

	/**
	 * Disconnect the client from all stores.
	 */
	public void close() throws RuntimeException {
		for (String rc : stores.keySet()) {
			for (T store : stores.get(rc).values()) {
				try {
					store.close();
				} catch (Exception e) {
					throw new RuntimeException(String.format("%s:%s: %s", store.getClusterID(), store.getStoreID(), e.getMessage()));
				}
			}
		}
		stores.clear();
		rc = null;
	}
	
	class UpdateState {
		Timestamps timestamps = new Timestamps();
		LinkedList<T> stores = new LinkedList<T>();
	}
	
	private UpdateState getUpdateState(String clusterID) {
		// Collect all timestamps from all online stores in the cluster.
		UpdateState result = new UpdateState();
		LinkedList<Timestamps> timestamps = new LinkedList<Timestamps>();
		for (T store : stores.get(clusterID).values()) {
			try {
				timestamps.add(store.getTimestamps());
				result.stores.add(store);
			} catch (Exception e) {
				System.err.println(String.format("%s:%s: %s", store.getClusterID(), store.getStoreID(), e.getMessage()));
			}
		}
		
		// Compute the common timestamps.
		result.timestamps = timestamps.removeFirst();
		for (String rc : stores.keySet()) {
			for (String rs : stores.get(rc).keySet()) {
				if (!timestamps.isEmpty()) {
					for (Timestamps t : timestamps) {
						if (rc.equals(clusterID))
							result.timestamps.set(rc, rs, Math.max(result.timestamps.get(rc, rs), t.get(rc, rs)));
						else
							result.timestamps.set(rc, rs, Math.min(result.timestamps.get(rc, rs), t.get(rc, rs)));
					}
				}
				else {
					result.timestamps.set(rc, rs, Math.max(result.timestamps.get(rc, rs), 0));
				}
			}
		}
		
		return result;
	}

	
	/**
	 * Pull all updates from a remote cluster and distribute them to the local cluster.
	 * @param clusterID	ID of the remote cluster.
	 * @param numThreads Number of threads in the thread pool for fetching and inserting updates. Each thread will process one store.
	 */
	public UpdateStats pullUpdates(String clusterID, int numThreads) {
		if (rc.equals(clusterID))
			return null;

		UpdateStats stats = new UpdateStats();
		
		// Get update states for local and remote clusters.
		final UpdateState local = getUpdateState(rc);
		UpdateState remote;
		boolean error;
		LinkedList<Element> updates;
		
		do {
			error = false;
			remote = getUpdateState(clusterID);
			updates = new LinkedList<Element>();
			stats.getUpdateIDs = stats.getUpdateElements = 0;
			ExecutorService pool = Executors.newFixedThreadPool(numThreads);
			class FutureResult {
				public List<Element> updates;
				public UpdateStats stats;
			}
			LinkedList<Future<FutureResult>> futures = new LinkedList<Future<FutureResult>>();
			for (final T store : remote.stores) {
				futures.add(pool.submit(new Callable<FutureResult>() {
					@Override
					public FutureResult call() throws Exception {
						FutureResult result = new FutureResult();
						result.updates = store.getUpdates(local.timestamps);
						result.stats = store.updateStats;
						return result;
					}
				}));
			}
			for (Future<FutureResult> future : futures) {
			    try {
			    	FutureResult result = future.get();
					updates.addAll(result.updates);
					stats.getUpdateIDs += result.stats.getUpdateIDs;
					stats.getUpdateElements += result.stats.getUpdateElements;
				} catch (Exception e) {
					System.err.println("Error while fetching updates: " + e.getMessage());
					System.err.println("Retrying...");
					error = true;
				}
			}
			stats.getUpdateIDs /= futures.size();
			stats.getUpdateElements /= futures.size();
		} while (error);
		

		// Distribute updates to the corresponding local stores.
		final HashMap<T, LinkedList<Element>> updatesMap = new HashMap<T, LinkedList<Element>>();
		for (Element u : updates) {
			T store = hash.get(u.value);
			LinkedList<Element> updatesForStore = updatesMap.get(store);
			if (updatesForStore == null) {
				updatesForStore = new LinkedList<Element>();
				updatesMap.put(store, updatesForStore);
			}
			updatesForStore.add(u);
		}
		
		// Save updates in the local stores and update the timestamps.
		stats.addUpdateElements = 0;
		ExecutorService pool = Executors.newFixedThreadPool(numThreads);
		LinkedList<Future<UpdateStats>> futures = new LinkedList<Future<UpdateStats>>();
		final UpdateState remoteFinal = remote;
		for (final T store : local.stores) {
			futures.add(pool.submit(new Callable<UpdateStats>() {
				@Override
				public UpdateStats call() throws Exception {
					store.addUpdates(updatesMap.get(store));
					store.updateMaxTimestamps(remoteFinal.timestamps);
					return store.updateStats;
				}
			}));
		}
		for (Future<UpdateStats> future : futures) {
		    try {
				stats.addUpdateElements += future.get().addUpdateElements;
			} catch (Exception e) {
				System.err.println("Error while adding updates: " + e.getMessage());
			}
		}
		stats.addUpdateElements /= futures.size();
		
		return stats;
	}
	
	public UpdateStats pullUpdates(String clusterID) {
		return pullUpdates(clusterID, 1);
	}
	
//	@Override
//	public String toString() {
//		StringBuilder sb = new StringBuilder();
//		for (T store : stores.get(rc).values()) {
//			sb.append(String.format("%s\n", store));
//		}
//		return sb.toString();
//	}
}
