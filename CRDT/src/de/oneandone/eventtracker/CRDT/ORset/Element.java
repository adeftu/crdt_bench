package de.oneandone.eventtracker.CRDT.ORset;

import java.io.Serializable;

/**
 * Representation of a tuple.
 * @author adeftu
 *
 */
public class Element implements Serializable {
	private static final long serialVersionUID = 6845807975082331940L;
	private static int TTL = -1;	// TTL of an element in seconds. -1 to disable.

	public String value;
	public String id;

	public Long added_t;
	public String added_rc;
	public String added_rs;

	public Long removed_t;
	public String removed_rc;
	public String removed_rs;
	
	public long gc_time;		// Start time for TTL (heap store).
	public int gc_ttl;			// Remaining TTL (redis store).

	/**
	 * Set TTL in seconds. Negative values mean no expiration.
	 */
	public static void setTTL(int ttl) {
		TTL = ttl;
	}
	
	public static int getTTL() {
		return TTL;
	}
	
	public Element(String value, 
				   Long added_t, String added_rc, String added_rs, 
				   Long removed_t, String removed_rc, String removed_rs) {
		this.value = value;

		this.added_t = added_t;
		this.added_rc = added_rc;
		this.added_rs = added_rs;

		this.removed_t = removed_t;
		this.removed_rc = removed_rc;
		this.removed_rs = removed_rs;
	}
	
	public Element(String value, 
				   Long added_t, String added_rc, String added_rs, 
				   Long removed_t, String removed_rc, String removed_rs, int ttl, String id) {
		this(value, added_t, added_rc, added_rs, removed_t, removed_rc, removed_rs);
		this.gc_ttl = ttl;
		this.id = id;
	}
	
	@Override
	public String toString() {
		return String.format("(%s, %d, %s, %s, %d, %s, %s)", value, added_t, added_rc, added_rs, removed_t, removed_rc, removed_rs);
	}
}
