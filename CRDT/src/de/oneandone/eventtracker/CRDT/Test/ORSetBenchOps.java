package de.oneandone.eventtracker.CRDT.Test;

import java.io.FileInputStream;
import java.util.ArrayList;
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
import de.oneandone.eventtracker.CRDT.ORset.Stores.RedisStore;

public class ORSetBenchOps {
	private static final String PROP_TOPOLOGY = "topology";
	private static final String PROP_BOOT = "boot";
	private static final String PROP_THREADS = "threads";
	private static final String PROP_NUM_OPS = "num_ops";
	private static final String PROP_VALUE_SIZE = "value_size";
	
	private Client<RedisStore> client;
	private Properties config;
	
	@Before
	public void init() throws Exception {
		client = new Client<RedisStore>(RedisStore.class); 
		
		config = new Properties();
		config.load(new FileInputStream("etc/orset/bench_ops.properties"));
		
		RedisStore.clearAndSetTopology(config.getProperty(PROP_TOPOLOGY));
		client.boot(config.getProperty(PROP_BOOT).split(":")[0], Integer.parseInt(config.getProperty(PROP_BOOT).split(":")[1]));
		
		client.setCheckIfStoresOnline(false);
	}
	
	@After
	public void clean() {
		client.clear(); 
		client.close();
	}
	
	@Test
	public void benchOps() throws InterruptedException, ExecutionException {
		Element.setTTL(-1);
		
		final int numOps = Integer.parseInt(config.getProperty(PROP_NUM_OPS));
		final int valueSize = Integer.parseInt(config.getProperty(PROP_VALUE_SIZE));
		ArrayList<String> values = new ArrayList<String>(numOps);
		for (int i = 0; i < numOps; ++i) {
			String value;
			do {
				byte[] bytes = new byte[valueSize];
				new Random().nextBytes(bytes);
				value = new String(bytes);
			} while (value.contains(":"));
			values.add(value);
		}

		ExecutorService pool;
		LinkedList<Future<?>> futures;
		long startTime;
		final int numThreads = Integer.parseInt(config.getProperty(PROP_THREADS));
		
		// ADD
		pool = Executors.newFixedThreadPool(numThreads);
		futures = new LinkedList<Future<?>>();
		startTime = System.nanoTime();
		for (final String value : values) {
			futures.add(pool.submit(new Runnable() {
				@Override
				public void run() {
					client.add(value);
				}
			}));
		}
		for (Future<?> future : futures) {
		    future.get();
		}
		System.out.printf("[ADD] Elapsed time: %.2f (ms)\n", (System.nanoTime() - startTime) / 1000000.0);
		pool.shutdown();
		while (!pool.awaitTermination(60, TimeUnit.SECONDS));
		
		
		// LOOKUP
		pool = Executors.newFixedThreadPool(numThreads);
		futures = new LinkedList<Future<?>>();
		startTime = System.nanoTime();
		for (final String value : values) {
			futures.add(pool.submit(new Runnable() {
				@Override
				public void run() {
					client.lookup(value);
				}
			}));
		}
		for (Future<?> future : futures) {
		    future.get();
		}
		System.out.printf("[LOOKUP] Elapsed time: %.2f (ms)\n", (System.nanoTime() - startTime) / 1000000.0);
		pool.shutdown();
		while (!pool.awaitTermination(60, TimeUnit.SECONDS));

		// REMOVE
		pool = Executors.newFixedThreadPool(numThreads);
		futures = new LinkedList<Future<?>>();
		startTime = System.nanoTime();
		for (final String value : values) {
			futures.add(pool.submit(new Runnable() {
				@Override
				public void run() {
					client.remove(value);
				}
			}));
		}
		for (Future<?> future : futures) {
		    future.get();
		}
		System.out.printf("[REMOVE] Elapsed time: %.2f (ms)\n", (System.nanoTime() - startTime) / 1000000.0);
		pool.shutdown();
		while (!pool.awaitTermination(60, TimeUnit.SECONDS));
	}
}
