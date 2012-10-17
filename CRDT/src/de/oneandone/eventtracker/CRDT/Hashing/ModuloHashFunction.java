package de.oneandone.eventtracker.CRDT.Hashing;

import java.util.ArrayList;
import java.util.Collection;

public class ModuloHashFunction<T> implements HashFunction<T> {
	private final ArrayList<T> nodes = new ArrayList<T>();

	@Override
	public synchronized void add(T node) {
		this.nodes.add(node);
	}

	@Override
	public synchronized void add(Collection<T> nodes) {
		this.nodes.addAll(nodes);
	}

	@Override
	public synchronized void remove(T node) {
		this.nodes.remove(node);
	}

	@Override
	public synchronized T get(String key) {
		return this.nodes.get(Math.abs(key.hashCode() % nodes.size()));
	}

}
