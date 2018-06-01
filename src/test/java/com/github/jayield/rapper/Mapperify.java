package com.github.jayield.rapper;

import com.github.jayield.rapper.utils.Pair;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class Mapperify<T extends DomainObject<K>, K> implements Mapper<T, K> {

    private final Mapper<T, K> other;
    private final ICounter<Void, CompletableFuture<List<T>>> ifindAll;
    private final ICounter<K, CompletableFuture<Optional<T>>> ifindById;
    private final ICounter<Pair<String, Object>[], CompletableFuture<List<T>>> ifindWhere;

    public Mapperify(Mapper<T, K> other){

        this.other = other;
        ifindById = Countify.of(other::findById);
        ifindAll = Countify.of(i -> other.findAll());
        ifindWhere = Countify.<Pair<String, Object>[], CompletableFuture<List<T>>>of(values -> other.findWhere(values));
    }

    @Override
    public <R> CompletableFuture<List<T>> findWhere(Pair<String, R>... values) {
        return ifindWhere.apply((Pair<String, Object>[]) values);
    }

    @Override
    public <R> CompletableFuture<List<T>> findWhere(int page, int numberOfItems, Pair<String, R>... values) {
        return other.findWhere(page, numberOfItems, values);
    }

    @Override
    public CompletableFuture<Optional<T>> findById(K k) {
        return ifindById.apply(k);
    }

    @Override
    public CompletableFuture<List<T>> findAll() {
        return ifindAll.apply(null);
    }

    @Override
    public CompletableFuture<List<T>> findAll(int page, int numberOfItems) {
        return other.findAll(page, numberOfItems);
    }

    @Override
    public CompletableFuture<Void> create(T t) {
        return other.create(t);
    }

    @Override
    public CompletableFuture<Void> createAll(Iterable<T> t) {
        return other.createAll(t);
    }

    @Override
    public CompletableFuture<Void> update(T t) {
        return other.update(t);
    }

    @Override
    public CompletableFuture<Void> updateAll(Iterable<T> t) {
        return other.updateAll(t);
    }

    @Override
    public CompletableFuture<Void> deleteById(K k) {
        return other.deleteById(k);
    }

    @Override
    public CompletableFuture<Void> delete(T t) {
        return other.delete(t);
    }

    @Override
    public CompletableFuture<Void> deleteAll(Iterable<K> keys) {
        return other.deleteAll(keys);
    }

    public ICounter<Void, CompletableFuture<List<T>>> getIfindAll() {
        return ifindAll;
    }

    public ICounter<K, CompletableFuture<Optional<T>>> getIfindById() {
        return ifindById;
    }

    public ICounter<Pair<String, Object>[], CompletableFuture<List<T>>> getIfindWhere() {
        return ifindWhere;
    }
}
