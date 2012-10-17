package de.oneandone.eventtracker.CRDT.ORset;

import java.net.InetSocketAddress;
import java.util.List;


/**
 * Base class for OR-set stores.
 * A store represents a DB server which hosts a subset (a shard) of the whole OR-set.
 * There are many such stores in a replica cluster, all holding disjunctive subsets according 
 * to some hash distribution function. Each cluster can pull missing updates from another
 * cluster of the same replica according to the delta-sync algorithm.
 * @author adeftu
 *
 */
public abstract class Store {
	protected String rc;
	protected String rs;
	protected InetSocketAddress address;
	protected boolean online = true;			// For testing purposes.
	protected boolean checkIfOnline = false;	// For testing purposes.
	
	public UpdateStats updateStats = new UpdateStats();
	

	/**
	 * Create a new store.
	 * @param address	Address of the database store server.
	 */
	public Store(InetSocketAddress address) {
		this.address = address;
	}
	
	
	/**
	 * Create a new store.
	 * @param rc		ID of the replica cluster.
	 * @param rs		ID of the replica store.
	 * @param address	Address of the database store server.
	 */
	public Store(String rc, String rs, InetSocketAddress address) {
		this.rc = rc;
		this.rs = rs;
		this.address = address;
	}
	
	
	public String getClusterID() {
		return rc;
	}


	public String getStoreID() {
		return rs;
	}


	public InetSocketAddress getAddress() {
		return address;
	}

	public void setOnline(boolean online) {
		this.online = online;
	}

	public void setCheckIfOnline(boolean checkIfOnline) {
		this.checkIfOnline = checkIfOnline;
	}
	
	/**
	 * Get the topology of the clusters and stores.
	 */
	public abstract Topology getTopology() throws Exception;
	
	
	/**
	 * Add an element to the store.
	 * Precondition: The element must be sharded here, i.e. the element must belong to this store according to the distribution function.
	 * Functionality:
	 * 1. Increment the timestamp of the store: T[added_rc][added_rs] = T[added_rc][added_rs] + 1 = added_t.
	 * 2. Add the Element(value, added_t, added_rc, added_rs, null, null, null) into the DB.
	 * where added_rc == this.rc and added_rs == this.rs
	 * Requirements:
	 * - 1. should execute atomically and in isolation.
	 * - 2. should execute atomically and in isolation.
	 * - 1. and 2. should execute in isolation with other DB operations.
	 * - It is allowed for 1. to succeed and 2. to fail but not the other way around.
	 * - If 2. fails, the client should retry add().
	 * @param value			The value to be added.
	 */
	public abstract void add(String value) throws Exception;
	
	
	/**
	 * Remove an element from the store.
	 * Precondition: The element must be sharded here, i.e. the element must belong to this store according to the distribution function.
	 * Functionality:
	 * 1. Increment the timestamp of the store: T[removed_rc][removed_rs] = T[removed_rc][removed_rs] + 1 = removed_t.
	 * 2. Update each Element(value, added_t, added_rc, added_rs, null, null, null) 
	 * to Element(value, added_t, added_rc, added_rs, removed_t, removed_rc, removed_rs) into the DB.
	 * where removed_rc == this.rc and removed_rs == this.rs
	 * Requirements:
	 * - 1. should execute atomically and in isolation.
	 * - 2. should execute atomically and in isolation.
	 * - 1. and 2. should execute in isolation with other DB operations.
	 * - It is allowed for 1. to succeed and 2. to fail but not the other way around.
	 * - If 2. fails, the client should retry remove().
	 * @param value	The value to be removed.
	 */
	public abstract void remove(String value) throws Exception;
	
	
	/**
	 * Search an element in the store.
	 * Functionality:
	 * Element is in store if:
	 * 1. exists Element(value, added_t, added_rc, added_rs, null, null, null) in DB
	 * AND
	 * 2. doesn't exist Element(value, added_t, added_rc, added_rs, removed_t, removed_rc, removed_rs) in DB
	 * Requirements:
	 * - Check for conditions 1. and 2. does not necessary need to be executed in isolation, 
	 * but in this situation T should be cached, otherwise false positives or false negatives may occur.
	 * @param value	The value to be searched.
	 * @return		'true' if found and 'false' otherwise.
	 */
	public abstract boolean lookup(String value) throws Exception;
	
	
	/**
	 * Get all current timestamps of this store.
	 * Functionality:
	 * 1. Get T[rc][rs] for all rc and all rs.
	 * Requirements:
	 * - Fetching all cells of T does not have to be done atomically and in isolation.
	 * @return	All timestamps of this store.
	 */
	public abstract Timestamps getTimestamps() throws Exception;
	
	
	/**
	 * Update current timestamps as current := max(current, other).
	 * Functionality:
	 * 1. T[rc][rs] := max(T[rc][rs], other[rc][rs]).
	 * Requirements:
	 * - Each cell should be updated atomically and isolation.
	 * - Updating all cells does not have to be done atomically and in isolation.
	 * - To have a safe view of updates and timestamps (i.e. not to skip some updates), addUpdates() must be called and succeed before.
	 * @param other	The other timestamps to update the current ones with.
	 */
	public abstract void updateMaxTimestamps(Timestamps other) throws Exception;

	
	/**
	 * Get all updates (add and remove) occurred in the store after the given timestamps.
	 * Functionality:
	 * 1. Get all Element(value, added_t, added_rc, added_rs, _, _, _), s.t. 
	 * if (removed_t == null) (timestamps[added_rc][added_rs] < added_t) else (timestamps[removed_rc][removed_rs] < removed_t)
	 * Requirements:
	 * - To have a safe view of updates and timestamps (i.e. not to skip some updates), getTimestamps() must be called and succeed before.
	 * @param timestamps	The starting time to get updates after.
	 * @return				Updates after the given timestamps.
	 */
	public abstract List<Element> getUpdates(Timestamps timestamps) throws Exception;

	
	/**
	 * Add updates to the store.
	 * Precondition: All elements must be sharded here, i.e. the elements must belong to this store according to the distribution function.
	 * Functionality:
	 * 1. Add all Element(...) to the DB. 
	 * Requirements:
	 * - Local timestamps must not be updated.
	 * - updateMaxTimestamps() should be called at the end but only if all updates were successfully added.
	 * @param updates	Updates to be added.
	 */
	public abstract void addUpdates(List<Element> updates) throws Exception;
	
	
	/**
	 * Clear the store database and timestamps.
	 */
	public abstract void clear() throws Exception;
	
	/**
	 * Close the connection to the store.
	 */
	public abstract void close();
}
