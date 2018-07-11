package com.github.jayield.rapper;

import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.utils.CollectionUtils;
import com.github.jayield.rapper.utils.Pair;
import com.github.jayield.rapper.utils.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class DataRepository<T extends DomainObject<K>, K> implements Mapper<T, K> {
    private static final Logger logger = LoggerFactory.getLogger(DataRepository.class);

    //private final ConcurrentMap<K, CompletableFuture<T>> identityMap = new ConcurrentHashMap<>();
    private final Class<T> type;
    private final Mapper<T, K> mapper;    //Used to communicate with the DB
    private final Comparator<T> comparator;

    public DataRepository(Class<T> type, Mapper<T, K> mapper, Comparator<T> comparator) {
        this.type = type;
        this.mapper = mapper;
        this.comparator = comparator;
    }

    public Mapper<T, K> getMapper() {
        return mapper;
    }

    @Override
    public <R> CompletableFuture<Long> getNumberOfEntries(UnitOfWork unit, Pair<String, R>... values) {
        return mapper.getNumberOfEntries(unit, values);
    }

    @Override
    public CompletableFuture<Long> getNumberOfEntries(UnitOfWork unit) {
        return mapper.getNumberOfEntries(unit);
    }

    @Override
    public <R> CompletableFuture<List<T>> findWhere(UnitOfWork unit, Pair<String, R>... values) {
        return find(unit, () -> mapper.findWhere(unit, values));
    }

    @Override
    public <R> CompletableFuture<List<T>> findWhere(UnitOfWork unit, int page, int numberOfItems, Pair<String, R>... values) {
        return find(unit, () -> mapper.findWhere(unit, page, numberOfItems, values));
    }

    @Override
    public CompletableFuture<Optional<T>> findById(UnitOfWork unit, K k) {
        CompletableFuture<T> completableFuture = unit
                .getIdentityMap(type)
                .computeIfAbsent(k, k1 ->
                        mapper.findById(unit, k)
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
    public CompletableFuture<List<T>> findAll(UnitOfWork unit) {
        return find(unit, () -> mapper.findAll(unit));
    }

    @Override
    public CompletableFuture<List<T>> findAll(UnitOfWork unit, int page, int numberOfItems) {
        return find(unit, () -> mapper.findAll(unit, page, numberOfItems));
    }

    @Override
    public CompletableFuture<Void> create(UnitOfWork unit, T t) {
        unit.registerNew(t);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> createAll(UnitOfWork unit, Iterable<T> t) {
        t.forEach(unit::registerNew);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> update(UnitOfWork unit, T t) {
        unit.registerDirty(t);

        CompletableFuture<? extends DomainObject> future = unit.getIdentityMap(type).computeIfPresent(t.getIdentityKey(), (k, tCompletableFuture) ->
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
                        });
    }

    @Override
    public CompletableFuture<Void> updateAll(UnitOfWork unit, Iterable<T> t) {
        List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
        t.forEach(t1 -> completableFutures.add(update(unit, t1)));
        return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]));
    }

    @Override
    public CompletableFuture<Void> deleteById(UnitOfWork unit, K k) {
        CompletableFuture<? extends DomainObject> future = unit.getIdentityMap(type).computeIfPresent(k, (key, tCompletableFuture) -> tCompletableFuture.thenApply(t -> {
            unit.registerRemoved(t);
            return t;
        }));

        return future != null ? future.thenApply(t -> null) :
                findById(unit, k)
                        .thenAccept(t -> {
                            T t1 = t.orElseThrow(() -> new DataMapperException("Object to delete was not found"));
                            unit.registerRemoved(t1);
                        });
    }

    @Override
    public CompletableFuture<Void> delete(UnitOfWork unit, T t) {
        unit.registerRemoved(t);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteAll(UnitOfWork unit, Iterable<K> keys) {
        List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
        keys.forEach(k -> completableFutures.add(deleteById(unit, k)));
        return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]));
    }

    private CompletableFuture<List<T>> find(UnitOfWork unit, Supplier<CompletableFuture<List<T>>> supplier){
        return supplier.get()
                .thenApply(list -> unit.processNewObjects(type, list, comparator))
                .thenCompose(CollectionUtils::listToCompletableFuture);
    }
}
