package org.github.isel.rapper;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * This will be the wall between memory and the DB.
 * Each method will go to the DB
 * @param <T>
 * @param <K>
 */
public interface Mapper<T extends DomainObject<K>, K> {
    CompletableFuture<Optional<T>> getById(K id);
    CompletableFuture<List<T>> getAll();
    void insert(T obj);
    void update(T obj);
    void delete(T obj);
}