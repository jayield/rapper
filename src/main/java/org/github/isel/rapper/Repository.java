package org.github.isel.rapper;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * An abstraction exported to the user
 * This interface will interact with the objects in-memory and if needed with Mapper to communicate with the DB
 * @param <T>
 * @param <K>
 */
public interface Repository<T extends DomainObject<K>, K> {
    CompletableFuture<Optional<T>> findById(K k);
    CompletableFuture<List<T>> findAll();
    CompletableFuture<Void> create(T t);
    CompletableFuture<Void> createAll(Iterable<T> t);
    CompletableFuture<Void> update(T t);
    CompletableFuture<Void> updateAll(Iterable<T> t);
    CompletableFuture<Void> deleteById(K k);
    CompletableFuture<Void> delete(T t);
    CompletableFuture<Void> deleteAll(Iterable<K> keys);
}

