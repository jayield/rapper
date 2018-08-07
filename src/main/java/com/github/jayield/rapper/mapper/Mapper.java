package com.github.jayield.rapper.mapper;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.utils.Condition;
import com.github.jayield.rapper.utils.Pair;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface Mapper<T extends DomainObject<K>, K> {

    /**
     * It will query the DB for how many entries does the table have where the condition matches.
     * @return number of entries that the table currently has.
     */
    CompletableFuture<Long> getNumberOfEntries(Condition<?>... values);

    /**
     * It will query the DB for how many entries does the table have.
     * @return number of entries that the table currently has.
     */
    CompletableFuture<Long> getNumberOfEntries();

    /**
     * It will try to locate T with the given properties passed in values
     * @param values a pair containing the properties to search T, the key must be the name of the column and the value the expected value of the column
     * @return a list of T's which match with the properties passed
     */
    CompletableFuture<List<T>> find(Condition<?>... values);

    /**
     * It will try to locate T with the given properties passed in values
     * @param page page to locate the T's
     * @param values a pair containing the properties to search T, the key must be the name of the column and the value the expected value of the column
     * @return a list of T's which match with the properties passed
     */
    CompletableFuture<List<T>> find(int page, int numberOfItems, Condition<?>... values);

    /**
     * Tries to locate T with the given key K
     * @param k key of T
     * @return Optional of T
     */
    CompletableFuture<Optional<T>> findById(K k);

    /**
     * It will insert t into the database
     * @param t new object to be inserted
     * @return CompletableFuture
     */
    CompletableFuture<Void> create(T t);

    /**
     * It will insert all t's passed in the parameters
     * @param t iterable with all the t's to be inserted
     * @return CompletableFuture
     */
    CompletableFuture<Void> createAll(Iterable<T> t);

    /**
     * It will update in the persistent memory the T which matches with the given t's key
     * @param t T to be updated
     * @return CompletableFuture
     */
    CompletableFuture<Void> update(T t);

    /**
     * It will update in the persistent memory all the T's which match with the given T's keys
     * @param t T's to be updated
     * @return CompletableFuture
     */
    CompletableFuture<Void> updateAll(Iterable<T> t);

    /**
     * It will remove from persistent memory the T identified by the key K
     * @param k key which identifies the object in persistent memory
     * @return CompletableFuture
     */
    CompletableFuture<Void> deleteById(K k);

    /**
     * It will remove from persistent memory the T which matches with the given T's key
     * @param t
     * @return
     */
    CompletableFuture<Void> delete(T t);

    /**
     * It will remove from persistent memory all the T's which matches with the given T's keys
     * @param keys
     * @return
     */
    CompletableFuture<Void> deleteAll(Iterable<K> keys);
}