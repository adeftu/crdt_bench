package de.oneandone.eventtracker.CRDT.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import de.oneandone.eventtracker.CRDT.ORset.Client;
import de.oneandone.eventtracker.CRDT.ORset.Element;
import de.oneandone.eventtracker.CRDT.ORset.Stores.HeapStore;
import de.oneandone.eventtracker.CRDT.ORset.Stores.RedisStore;

@RunWith(Parameterized.class)
public class ORSetUnitTest {
	
	@SuppressWarnings("rawtypes")
	private Client clientA, clientB, clientC;
	private StoreType storeType;
	private TopologyType topologyType;
	
	static enum StoreType {
		HEAP,
		REDIS
	}
	
	static enum TopologyType {
		SINGLE,
		MULTI
	}
	
	public ORSetUnitTest(StoreType storeType, TopologyType topologyType) {
		this.storeType = storeType;
		this.topologyType = topologyType;
	}
	
	@Parameters
	public static Collection<Object[]> parameters() {
		return Arrays.asList(new Object[][] {{StoreType.HEAP, TopologyType.MULTI}, {StoreType.REDIS, TopologyType.MULTI} });
	}
	
	@Before
	public void init() {
		switch (storeType) {
		case HEAP:
			clientA = new Client<HeapStore>(HeapStore.class);
			clientB = new Client<HeapStore>(HeapStore.class);
			clientC = new Client<HeapStore>(HeapStore.class);
			try {
				switch (topologyType) {
				case SINGLE:
					HeapStore.clearAndSetTopology("etc/orset/topology_single.xml");
					break;
				case MULTI:
					HeapStore.clearAndSetTopology("etc/orset/topology_multi.xml");
					break;
				}
			} catch (Exception e) {
				System.err.println(e.getMessage());
			}
			break;
		case REDIS:
			clientA = new Client<RedisStore>(RedisStore.class);
			clientB = new Client<RedisStore>(RedisStore.class);
			clientC = new Client<RedisStore>(RedisStore.class);
			try {
				switch (topologyType) {
				case SINGLE:
					RedisStore.clearAndSetTopology("etc/orset/topology_single.xml");
					break;
				case MULTI:
					RedisStore.clearAndSetTopology("etc/orset/topology_multi.xml");
					break;
				}
			} catch (Exception e) {
				System.err.println(e.getMessage());
			}
			break;
		}
		
		try {
			clientA.boot("127.0.0.1", 6379); clientA.setCheckIfStoresOnline(true);
			clientB.boot("127.0.0.1", 6380); clientB.setCheckIfStoresOnline(true);
			clientC.boot("127.0.0.1", 6381); clientC.setCheckIfStoresOnline(true);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
	@After
	public void clean() {
		clientA.clear(); clientA.close();
		clientB.clear(); clientB.close();
		clientC.clear(); clientC.close();
	}
	
	/**
	 * Add a value and assert existence.
	 */
	@SuppressWarnings("rawtypes")
	private void add(Client client, String value) {
		client.add(value);
		assertTrue(client.lookup(value));
	}

	/**
	 * Add a value, sleep and assert (non-)existence.
	 */
	@SuppressWarnings("rawtypes")
	private void addAndSleep(Client client, String value, int sleep, boolean exists) {
		client.add(value);
		try {
			Thread.sleep((sleep + 1) * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (exists)
			assertTrue(client.lookup(value));
		else
			assertFalse(client.lookup(value));
	}
	
	/**
	 * Pull updates from source to destination and assert (non-)existence on destination.
	 */
	@SuppressWarnings("rawtypes")
	private void pull(Client dest, Client src, String value, boolean exists) {
		dest.pullUpdates(src.getClusterID());
		if (exists)
			assertTrue(dest.lookup(value));
		else
			assertFalse(dest.lookup(value));
	}
	
	
	/**
	 * Pull updates from source to destination, sleep and assert (non-)existence on destination.
	 */
	@SuppressWarnings("rawtypes")
	private void pullAndSleep(Client dest, Client src, String value, int sleep, boolean exists) {
		dest.pullUpdates(src.getClusterID());
		try {
			Thread.sleep((sleep + 1) * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (exists)
			assertTrue(dest.lookup(value));
		else
			assertFalse(dest.lookup(value));
	}
	
	
	/**
	 * Remove a value and assert non-existence.
	 */
	@SuppressWarnings("rawtypes")
	private void remove(Client client, String value) {
		client.remove(value);
		assertFalse(client.lookup(value));
	}
	
	
	/**
	 * Remove a value, sleep and assert (non-)existence.
	 */
	@SuppressWarnings("rawtypes")
	private void removeAndSleep(Client client, String value, int sleep, boolean exists) {
		client.remove(value);
		try {
			Thread.sleep((sleep + 1) * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (exists)
			assertTrue(client.lookup(value));
		else
			assertFalse(client.lookup(value));
	}
	

	/**
	 * ADD
	 */
	@Test
	public void basicTest1() {
		add(clientA, "a");
	}
	
	
	/**
	 * ADD + RMV
	 */
	@Test
	public void basicTest2() {
		add(clientA, "a");
		remove(clientA, "a");
	}
	
	
	/**
	 * ADD + ADD + RMV
	 */
	@Test
	public void basicTest3() {
		add(clientA, "a");
		add(clientA, "a");
		remove(clientA, "a");
	}
	
	
	/**
	 * RMV + ADD
	 */
	@Test
	public void basicTest4() {
		remove(clientA, "a");
		add(clientA, "a");
	}
	
	
	/**
	 * ADD propagates directly.
	 */
	@Test
	public void basicTest5() {
		add(clientA, "a");
		pull(clientB, clientA, "a", true);
	}
	
	
	/**
	 * ADD propagates indirectly.
	 */
	@Test
	public void basicTest6() {
		add(clientA, "a");
		pull(clientB, clientA, "a", true);
		pull(clientC, clientB, "a", true);
	}
	
	
	/**
	 * RMV propagates directly.
	 */
	@Test
	public void basicTest7() {
		add(clientA, "a");
		pull(clientB, clientA, "a", true);
		remove(clientA, "a");
		pull(clientB, clientA, "a", false);
	}
	
	
	/**
	 * RMV propagates indirectly.
	 */
	@Test
	public void basicTest8() {
		add(clientA, "a");
		pull(clientB, clientA, "a", true);
		pull(clientC, clientB, "a", true);
		remove(clientC, "a");
		pull(clientB, clientC, "a", false);
		pull(clientA, clientB, "a", false);
	}
	
	
	/**
	 * ADD || RMV directly.
	 */
	@Test
	public void basicTest9() {
		add(clientA, "a");
		add(clientB, "a");
		remove(clientA, "a");
		pull(clientB, clientA, "a", true);
	}
	
	
	/**
	 * ADD || RMV indirectly.
	 */
	@Test
	public void basicTest10() {
		add(clientA, "a");
		add(clientB, "a");
		pull(clientC, clientB, "a", true);
		pull(clientC, clientA, "a", true);
		remove(clientA, "a");
		pull(clientC, clientA, "a", true);
	}
	
	
	/**
	 * ADD propagation stops at the failed store.
	 */
	@Test
	public void failTest1() {
		add(clientA, "a");
		pull(clientB, clientA, "a", true);
		clientB.getStore("a").setOnline(false);
		pull(clientC, clientB, "a", false);
	}
	
	
	/**
	 * RMV propagation stops at the failed store.
	 */
	@Test
	public void failTest2() {
		add(clientA, "a");
		pull(clientB, clientA, "a", true);
		pull(clientC, clientB, "a", true);
		remove(clientA, "a");
		pull(clientB, clientA, "a", false);
		clientB.getStore("a").setOnline(false);
		pull(clientC, clientB, "a", true);
	}
	
	
	/**
	 * ADD propagates after source fails.
	 */
	@Test
	public void failTest3() {
		add(clientA, "a");
		clientA.getStore("a").setOnline(false);
		pull(clientB, clientA, "a", false);
		clientA.getStore("a").setOnline(true);
		pull(clientB, clientA, "a", true);
	}
	
	
	/**
	 * RMV propagates after source fails.
	 */
	@Test
	public void failTest4() {
		add(clientA, "a");
		pull(clientB, clientA, "a", true);
		remove(clientA, "a");
		clientA.getStore("a").setOnline(false);
		pull(clientB, clientA, "a", true);
		clientA.getStore("a").setOnline(true);
		pull(clientB, clientA, "a", false);
	}
	
	
	private boolean multiStoresClusters() {
		return clientA.getClusterSize() > 1 || clientB.getClusterSize() > 1 || clientC.getClusterSize() > 1;
	}
	
	/**
	 * Store failures in the source cluster.
	 */
	@SuppressWarnings("rawtypes")
	@Test
	public void failTest5() {
		if (!multiStoresClusters())
			return;
		Client src = clientA.getClusterSize() > 1 ? clientA : (clientB.getClusterSize() > 1 ? clientB : clientC);
		Client dst = src == clientA ? clientB : (src == clientB ? clientC : clientA);
		
		// Generate 2 values in different stores in src cluster.
		String value1 = String.valueOf((char)(new Random().nextInt(26) + 'a'));
		String value2 = null;
		do {
			value2 = String.valueOf((char)(new Random().nextInt(26) + 'a'));
		} while (src.getStore(value1).getStoreID().equals(src.getStore(value2).getStoreID()));
		
		add(src, value1);
		src.getStore(value2).setOnline(false);
		pull(dst, src, value1, true);
		src.getStore(value2).setOnline(true);
		add(src, value2);
		pull(dst, src, value2, true);
	}
	
	
	/**
	 * Store failures in the destination cluster.
	 */
	@SuppressWarnings("rawtypes")
	@Test
	public void failTest6() {
		if (!multiStoresClusters())
			return;
		Client dst = clientA.getClusterSize() > 1 ? clientA : (clientB.getClusterSize() > 1 ? clientB : clientC);
		Client src = dst == clientA ? clientB : (dst == clientB ? clientC : clientA);

		// Generate 2 values in different stores in dst cluster.
		String value1 = String.valueOf((char)(new Random().nextInt(26) + 'a'));
		String value2 = null;
		do {
			value2 = String.valueOf((char)(new Random().nextInt(26) + 'a'));
		} while (dst.getStore(value1).getStoreID().equals(dst.getStore(value2).getStoreID()));
			
		add(src, value1);	
		dst.getStore(value2).setOnline(false);
		pull(dst, src, value1, true);
		dst.getStore(value2).setOnline(true);
		add(src, value2);
		pull(dst, src, value2, true);
	}
	
	/**
	 * ADD
	 */
	@Test
	public void gcTest1() {
		Element.setTTL(1);
		addAndSleep(clientA, "a", Element.getTTL(), false);
	}
	
	
	/**
	 * ADD + RMV
	 */
	@Test
	public void gcTest2() {
		Element.setTTL(1);
		add(clientA, "a");
		removeAndSleep(clientA, "a", Element.getTTL(), false);
	}
	
	
	/**
	 * RMV + ADD
	 */
	@Test
	public void gcTest3() {
		Element.setTTL(1);
		add(clientA, "a");
		remove(clientA, "a");
		addAndSleep(clientA, "a", Element.getTTL(), false);
	}
	
	
	/**
	 * ADD expires after propagation
	 */
	@Test
	public void gcTest4() {
		Element.setTTL(1);
		add(clientA, "a");
		pullAndSleep(clientB, clientA, "a", Element.getTTL(), false);
	}
	
	
	/**
	 * ADD + RMV expires after propagation
	 */
	@Test
	public void gcTest5() {
		Element.setTTL(1);
		add(clientA, "a");
		remove(clientA, "a");
		pullAndSleep(clientB, clientA, "a", Element.getTTL(), false);
	}
	
	
	/**
	 * RMV + ADD expires after propagation
	 */
	@Test
	public void gcTest6() {
		Element.setTTL(1);
		add(clientA, "a");
		remove(clientA, "a");
		add(clientA, "a");
		pullAndSleep(clientB, clientA, "a", Element.getTTL(), false);
	}
}
