package com.github.jayield.rapper;

import com.github.jayield.rapper.utils.Pair;
import com.github.jayield.rapper.utils.UnitOfWork;

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
        ifindById = Countify.<K, CompletableFuture<Optional<T>>>of((unitOfWork, k) -> other.findById(unitOfWork, k));
        ifindAll = Countify.of((unit, i) -> other.findAll(unit));
        ifindWhere = Countify.<Pair<String, Object>[], CompletableFuture<List<T>>>of((unit, values) -> other.findWhere(unit, values));
    }

    @Override
    public <R> CompletableFuture<Long> getNumberOfEntries(UnitOfWork unit, Pair<String, R>... values) {
        return other.getNumberOfEntries(unit, values);
    }

    @Override
    public CompletableFuture<Long> getNumberOfEntries(UnitOfWork unit) {
        return other.getNumberOfEntries(unit);
    }

    @Override
    public <R> CompletableFuture<List<T>> findWhere(UnitOfWork unit, Pair<String, R>... values) {
        return ifindWhere.apply(unit, (Pair<String, Object>[]) values);
    }

    @Override
    public <R> CompletableFuture<List<T>> findWhere(UnitOfWork unit, int page, int numberOfItems, Pair<String, R>... values) {
        return other.findWhere(unit, page, numberOfItems, values);
    }

    @Override
    public CompletableFuture<Optional<T>> findById(UnitOfWork unit, K k) {
        return ifindById.apply(unit, k);
    }

    @Override
    public CompletableFuture<List<T>> findAll(UnitOfWork unit) {
        return ifindAll.apply(unit, null);
    }

    @Override
    public CompletableFuture<List<T>> findAll(UnitOfWork unit, int page, int numberOfItems) {
        return other.findAll(unit, page, numberOfItems);
    }

    @Override
    public CompletableFuture<Void> create(UnitOfWork unit, T t) {
        return other.create(unit, t);
    }

    @Override
    public CompletableFuture<Void> createAll(UnitOfWork unit, Iterable<T> t) {
        return other.createAll(unit, t);
    }

    @Override
    public CompletableFuture<Void> update(UnitOfWork unit, T t) {
        return other.update(unit, t);
    }

    @Override
    public CompletableFuture<Void> updateAll(UnitOfWork unit, Iterable<T> t) {
        return other.updateAll(unit, t);
    }

    @Override
    public CompletableFuture<Void> deleteById(UnitOfWork unit, K k) {
        return other.deleteById(unit, k);
    }

    @Override
    public CompletableFuture<Void> delete(UnitOfWork unit, T t) {
        return other.delete(unit, t);
    }

    @Override
    public CompletableFuture<Void> deleteAll(UnitOfWork unit, Iterable<K> keys) {
        return other.deleteAll(unit, keys);
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
