package com.github.jayield.rapper;

import javafx.util.Pair;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface Mapper<T extends DomainObject<K>, K> {

    /**
     * It will try to locate T with the given properties passed in values
     * @param values a pair containing the properties to search T, the key must be the name of the column and the value the expected value of the column
     * @param <R> The type of the column
     * @return a list of T's which match with the properties passed
     */
    <R> CompletableFuture<List<T>> findWhere(Pair<String, R>... values);

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
    CompletableFuture<Optional<Throwable>> create(T t);

    /**
     * It will insert all t's passed in the parameters
     * @param t iterable with all the t's to be inserted
     * @return CompletableFuture
     */
    CompletableFuture<Optional<Throwable>> createAll(Iterable<T> t);

    /**
     * It will update in the persistent memory the T which matches with the given t's key
     * @param t T to be updated
     * @return CompletableFuture
     */
    CompletableFuture<Optional<Throwable>> update(T t);

    /**
     * It will update in the persistent memory all the T's which match with the given T's keys
     * @param t T's to be updated
     * @return CompletableFuture
     */
    CompletableFuture<Optional<Throwable>> updateAll(Iterable<T> t);

    /**
     * It will remove from persistent memory the T identified by the key K
     * @param k key which identifies the object in persistent memory
     * @return CompletableFuture
     */
    CompletableFuture<Optional<Throwable>> deleteById(K k);

    /**
     * It will remove from persistent memory the T which matches with the given T's key
     * @param t
     * @return
     */
    CompletableFuture<Optional<Throwable>> delete(T t);

    /**
     * It will remove from persistent memory all the T's which matches with the given T's keys
     * @param keys
     * @return
     */
    CompletableFuture<Optional<Throwable>> deleteAll(Iterable<K> keys);
}