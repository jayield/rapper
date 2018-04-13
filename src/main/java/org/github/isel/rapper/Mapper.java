package org.github.isel.rapper;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface Mapper<T extends DomainObject<K>, K> {
    /**
     * Tries to locate T with the given key K
     * @param k key of T
     * @return Optional of T
     */
    CompletableFuture<Optional<T>> findById(K k);

    /**
     * Returns all T present in the database
     * @return List of all T present in the database
     */
    CompletableFuture<List<T>> findAll();

    /**
     * It will insert t into the database
     * @param t new object to be inserted
     * @return CompletableFuture
     */
    CompletableFuture<Boolean> create(T t);

    /**
     * It will insert all t's passed in the parameters
     * @param t iterable with all the t's to be inserted
     * @return CompletableFuture
     */
    CompletableFuture<Boolean> createAll(Iterable<T> t);

    /**
     * It will update in the persistent memory the T which matches with the given t's key
     * @param t T to be updated
     * @return CompletableFuture
     */
    CompletableFuture<Boolean> update(T t);

    /**
     * It will update in the persistent memory all the T's which match with the given T's keys
     * @param t T's to be updated
     * @return CompletableFuture
     */
    CompletableFuture<Boolean> updateAll(Iterable<T> t);

    /**
     * It will remove from persistent memory the T identified by the key K
     * @param k key which identifies the object in persistent memory
     * @return CompletableFuture
     */
    CompletableFuture<Boolean> deleteById(K k);

    /**
     * It will remove from persistent memory the T which matches with the given T's key
     * @param t
     * @return
     */
    CompletableFuture<Boolean> delete(T t);

    /**
     * It will remove from persistent memory all the T's which matches with the given T's keys
     * @param keys
     * @return
     */
    CompletableFuture<Boolean> deleteAll(Iterable<K> keys);
}