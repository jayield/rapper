package com.github.jayield.rapper;

import com.github.jayield.rapper.mapper.Mapper;
import com.github.jayield.rapper.utils.Condition;
import com.github.jayield.rapper.utils.Pair;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class Mapperify<T extends DomainObject<K>, K> implements Mapper<T, K> {

    private final Mapper<T, K> other;
    private final ICounter<K, CompletableFuture<Optional<T>>> ifindById;
    private final ICounter<Condition<?>[], CompletableFuture<List<T>>> ifind;

    public Mapperify(Mapper<T, K> other){
        this.other = other;
        ifindById = Countify.of(other::findById);
        ifind = Countify.of(other::find);
    }

    @Override
    public CompletableFuture<Long> getNumberOfEntries(Condition<?>... values) {
        return other.getNumberOfEntries(values);
    }

    @Override
    public CompletableFuture<Long> getNumberOfEntries() {
        return other.getNumberOfEntries();
    }

    @Override
    public CompletableFuture<List<T>> find(Condition<?>... values) {
        return ifind.apply(values);
    }

    @Override
    public CompletableFuture<List<T>> find(int page, int numberOfItems, Condition<?>... values) {
        return other.find(page, numberOfItems, values);
    }

    @Override
    public CompletableFuture<Optional<T>> findById(K k) {
        return ifindById.apply(k);
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

    public ICounter<K, CompletableFuture<Optional<T>>> getIfindById() {
        return ifindById;
    }

    public ICounter<Condition<?>[], CompletableFuture<List<T>>> getIfind() {
        return ifind;
    }
}
