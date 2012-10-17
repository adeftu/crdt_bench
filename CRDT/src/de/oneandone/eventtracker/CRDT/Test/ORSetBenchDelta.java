package de.oneandone.eventtracker.CRDT.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.oneandone.eventtracker.CRDT.ORset.Client;
import de.oneandone.eventtracker.CRDT.ORset.Element;
import de.oneandone.eventtracker.CRDT.ORset.UpdateStats;
import de.oneandone.eventtracker.CRDT.ORset.Stores.RedisStore;
import de.oneandone.eventtracker.CRDT.Utils.TrafficStats;
import de.oneandone.eventtracker.CRDT.Utils.TrafficStats.BytesStats;

public class ORSetBenchDelta {
	private static final String PROP_TOPOLOGY = "topology";
	private static final String PROP_BOOT_A = "boot_A";
	private static final String PROP_BOOT_B = "boot_B";
	private static final String PROP_NUM_VALUES_BETWEEN_PULLS = "num_values_between_pulls";
	private static final String PROP_NUM_PULLS = "num_pulls";
	private static final String PROP_VALUE_SIZE = "value_size";
	private static final String PROP_PULL_NUM_THREADS = "pull_num_threads";
		
	private Client<RedisStore> clientA, clientB;
	private Properties config;
	
	@Before
	public void init() throws Exception {
		clientA = new Client<RedisStore>(RedisStore.class);	
		clientB = new Client<RedisStore>(RedisStore.class);
		
		config = new Properties();
		config.load(new FileInputStream("etc/orset/bench_delta.properties"));
		
		RedisStore.clearAndSetTopology(config.getProperty(PROP_TOPOLOGY));
		
		clientA.boot(config.getProperty(PROP_BOOT_A).split(":")[0], Integer.parseInt(config.getProperty(PROP_BOOT_A).split(":")[1]));
		clientB.boot(config.getProperty(PROP_BOOT_B).split(":")[0], Integer.parseInt(config.getProperty(PROP_BOOT_B).split(":")[1]));
		
		clientA.setCheckIfStoresOnline(false);
		clientB.setCheckIfStoresOnline(false);
	}
	
	@After
	public void clean() {
		clientA.clear(); clientA.close();
		clientB.clear(); clientB.close();
	}
	
	private void addValues(final Client<RedisStore> client, int numValues, final int valueSize) throws InterruptedException, ExecutionException {
		final Random random = new Random();
		ExecutorService pool = Executors.newFixedThreadPool(8);
		
		LinkedList<Future<?>> futures = new LinkedList<Future<?>>();
		for (int i = 0; i < numValues; ++i) {
			futures.add(pool.submit(new Runnable() {
				@Override
				public void run() {
					String value;
					do {
						byte[] bytes = new byte[valueSize];
						random.nextBytes(bytes);
						value = new String(bytes);
					} while (value.contains(":"));
					client.add(value);
				}
			}));
		}
		for (Future<?> future : futures) {
		    future.get();
		}
		pool.shutdown();
		while (!pool.awaitTermination(60, TimeUnit.SECONDS));
	}
	
	@Test
	public void benchDelta() throws IOException, InterruptedException, ExecutionException {
		final int numPulls = Integer.parseInt(config.getProperty(PROP_NUM_PULLS));
		final int numValuesBetweenPulls = Integer.parseInt(config.getProperty(PROP_NUM_VALUES_BETWEEN_PULLS));
		final int valueSize = Integer.parseInt(config.getProperty(PROP_VALUE_SIZE));
		Element.setTTL(-1);
		for(int i = 0; i < numPulls; ++i) {
			addValues(clientA, numValuesBetweenPulls, valueSize);
			BytesStats startBytesStats = TrafficStats.getBytesStats("eth0");
			long startTime = System.nanoTime();
			UpdateStats updateStats = clientB.pullUpdates(clientA.getClusterID(), Integer.parseInt(config.getProperty(PROP_PULL_NUM_THREADS)));
			BytesStats endBytesStats = TrafficStats.getBytesStats("eth0");
			System.out.printf("%.2f,%.2f,%.2f,%.2f,%d,%d\n", 
					updateStats.getUpdateIDs, updateStats.getUpdateElements, updateStats.addUpdateElements, (System.nanoTime() - startTime) / 1000000.0,
					endBytesStats.totalRxBytes - startBytesStats.totalRxBytes,
					endBytesStats.totalTxBytes - startBytesStats.totalTxBytes);
		}
	}
}
