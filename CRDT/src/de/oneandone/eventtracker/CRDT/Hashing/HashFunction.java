package de.oneandone.eventtracker.CRDT.Hashing;

import java.util.Collection;

public interface HashFunction<T> {
	public void add(T node);
	public void add(Collection<T> nodes);
	public void remove(T node);
	public T get(String key);
}
