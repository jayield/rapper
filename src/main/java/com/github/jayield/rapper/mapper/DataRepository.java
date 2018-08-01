package com.github.jayield.rapper.mapper;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.utils.CollectionUtils;
import com.github.jayield.rapper.utils.Pair;
import com.github.jayield.rapper.unitofwork.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class DataRepository<T extends DomainObject<K>, K> implements Mapper<T, K> {
    private static final Logger logger = LoggerFactory.getLogger(DataRepository.class);

    //private final ConcurrentMap<K, CompletableFuture<T>> identityMap = new ConcurrentHashMap<>();
    private final Class<T> type;
    private final Mapper<T, K> mapper;    //Used to communicate with the DB
    private final Comparator<T> comparator;
    private final UnitOfWork unit;

    public DataRepository(Class<T> type, Mapper<T, K> mapper, Comparator<T> comparator, UnitOfWork unit) {
        this.type = type;
        this.mapper = mapper;
        this.comparator = comparator;
        this.unit = unit;
    }

    public Mapper<T, K> getMapper() {
        return mapper;
    }

    @Override
    public <R> CompletableFuture<Long> getNumberOfEntries(Pair<String, R>... values) {
        return mapper.getNumberOfEntries(values);
    }

    @Override
    public CompletableFuture<Long> getNumberOfEntries() {
        return mapper.getNumberOfEntries();
    }

    @Override
    public <R> CompletableFuture<List<T>> findWhere(Pair<String, R>... values) {
        return find(() -> mapper.findWhere(values));
    }

    @Override
    public <R> CompletableFuture<List<T>> findWhere(int page, int numberOfItems, Pair<String, R>... values) {
        return find(() -> mapper.findWhere(page, numberOfItems, values));
    }

    @Override
    public CompletableFuture<Optional<T>> findById(K k) {
        CompletableFuture<T> completableFuture = unit
                .getIdentityMap(type)
                .computeIfAbsent(k, k1 ->
                        mapper.findById(k)
                                .thenApply(t -> t.orElseThrow(() -> new DataMapperException(type.getSimpleName() + " was not found")))
                )
                .thenApply(t -> (T)t);

        //else logger.info("{} with id {} obtained from IdentityMap", type.getSimpleName(), k);

        return completableFuture
                .thenApply(Optional::of)
                .exceptionally(throwable -> {
                    logger.warn("Removing CompletableFuture of {} from identityMap due to {}", type.getSimpleName(), throwable.getMessage());
                    unit.getIdentityMap(type).remove(k);
                    return Optional.empty();
                });
    }

    @Override
    public CompletableFuture<List<T>> findAll() {
        return find(mapper::findAll);
    }

    @Override
    public CompletableFuture<List<T>> findAll(int page, int numberOfItems) {
        return find(() -> mapper.findAll(page, numberOfItems));
    }

    @Override
    public CompletableFuture<Void> create(T t) {
        unit.registerNew(t);
        return mapper.create(t);
    }

    @Override
    public CompletableFuture<Void> createAll(Iterable<T> t) {
        t.forEach(unit::registerNew);
        return mapper.createAll(t);
    }

    @Override
    public CompletableFuture<Void> update(T t) {
        unit.registerDirty(t);

        /*CompletableFuture<? extends DomainObject> future = unit.getIdentityMap(type).computeIfPresent(t.getIdentityKey(), (k, tCompletableFuture) ->
                tCompletableFuture.thenApply(t2 -> {
                    unit.registerClone(t2);
                    return t2;
                })
        );

        return future != null ? future.thenApply(t1 -> null) :
                findById(unit, t.getIdentityKey())
                        .thenAccept(optionalT -> {
                            T t1 = optionalT.orElseThrow(() -> new DataMapperException(type.getSimpleName() + " not found"));
                            unit.registerClone(t1);
                        });*/
        return mapper.update(t);
    }

    @Override
    public CompletableFuture<Void> updateAll(Iterable<T> t) {
        /*List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
        t.forEach(t1 -> completableFutures.add(update(unit, t1)));
        return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]));*/
        t.forEach(unit::registerDirty);
        return mapper.updateAll(t);
    }

    @Override
    public CompletableFuture<Void> deleteById(K k) {
        CompletableFuture<? extends DomainObject> future = unit.getIdentityMap(type).computeIfPresent(k, (key, tCompletableFuture) -> tCompletableFuture.thenApply(t -> {
            unit.registerRemoved(t);
            return t;
        }));

        return future != null ? mapper.deleteById(k) : findById(k)
                .thenCompose(t -> {
                    T t1 = t.orElseThrow(() -> new DataMapperException("Object to delete was not found"));
                    unit.registerRemoved(t1);
                    return mapper.deleteById(k);
                });
    }

    @Override
    public CompletableFuture<Void> delete(T t) {
        unit.registerRemoved(t);
        return mapper.delete(t);
    }

    @Override
    public CompletableFuture<Void> deleteAll(Iterable<K> keys) {
        List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
        keys.forEach(k -> completableFutures.add(deleteById(k)));
        return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]));
    }

    private CompletableFuture<List<T>> find(Supplier<CompletableFuture<List<T>>> supplier){
        return supplier.get()
                .thenApply(list -> unit.processNewObjects(type, list, comparator))
                .thenCompose(CollectionUtils::listToCompletableFuture);
    }
}
