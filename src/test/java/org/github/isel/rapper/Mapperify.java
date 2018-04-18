package org.github.isel.rapper;

import javafx.util.Pair;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class Mapperify<T extends DomainObject<K>, K> implements Mapper<T, K> {

    private final Mapper<T, K> other;
    private final ICounter<Void, CompletableFuture<List<T>>> ifindAll;
    private final ICounter<K, CompletableFuture<Optional<T>>> ifindById;

    public Mapperify(Mapper<T, K> other){

        this.other = other;
        ifindById = Countify.of(other::findById);
        ifindAll = Countify.of(i -> other.findAll());
    }

    @Override
    public <R> CompletableFuture<List<T>> findWhere(Pair<String, R>... values) {
        return other.findWhere(values);
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
    public CompletableFuture<Boolean> create(T t) {
        return other.create(t);
    }

    @Override
    public CompletableFuture<Boolean> createAll(Iterable<T> t) {
        return other.createAll(t);
    }

    @Override
    public CompletableFuture<Boolean> update(T t) {
        return other.update(t);
    }

    @Override
    public CompletableFuture<Boolean> updateAll(Iterable<T> t) {
        return other.updateAll(t);
    }

    @Override
    public CompletableFuture<Boolean> deleteById(K k) {
        return other.deleteById(k);
    }

    @Override
    public CompletableFuture<Boolean> delete(T t) {
        return other.delete(t);
    }

    @Override
    public CompletableFuture<Boolean> deleteAll(Iterable<K> keys) {
        return other.deleteAll(keys);
    }

    public ICounter<Void, CompletableFuture<List<T>>> getIfindAll() {
        return ifindAll;
    }

    public ICounter<K, CompletableFuture<Optional<T>>> getIfindById() {
        return ifindById;
    }
}
