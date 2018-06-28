package com.github.jayield.rapper;

import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.exceptions.UnitOfWorkException;
import com.github.jayield.rapper.utils.*;
import io.vertx.ext.sql.SQLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.github.jayield.rapper.utils.ConnectionManager.*;

public class DataRepository<T extends DomainObject<K>, K> implements Mapper<T, K> {

    private static final Logger logger = LoggerFactory.getLogger(DataRepository.class);
    private final ConcurrentMap<K, CompletableFuture<T>> identityMap = new ConcurrentHashMap<>();
    private final Class<T> type;
    private final Class<K> keyType;
    private final Mapper<T, K> mapper;    //Used to communicate with the DB
    private final ExternalsHandler<T, K> externalsHandler;
    private final Comparator<T> comparator;

    public DataRepository(Class<T> type, Class<K> keyType, Mapper<T, K> mapper, ExternalsHandler<T, K> externalsHandler, Comparator<T> comparator) {
        this.type = type;
        this.keyType = keyType;
        this.mapper = mapper;
        this.externalsHandler = externalsHandler;
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
        return find(() -> mapper.findWhere(unit, values));
    }

    @Override
    public <R> CompletableFuture<List<T>> findWhere(UnitOfWork unit, int page, int numberOfItems, Pair<String, R>... values) {
        return find(() -> mapper.findWhere(unit, page, numberOfItems, values));
    }

    @Override
    public CompletableFuture<Optional<T>> findById(UnitOfWork unit, K k) {
        boolean[] wasComputed = {false};
        //System.out.println("k " + k + " identitymap " + identityMap);
        CompletableFuture<T> completableFuture = identityMap.computeIfAbsent(k, k1 -> {
            wasComputed[0] = true;
            return mapper.findById(unit, k)
                    .thenApply(t -> t.orElseThrow(() -> new DataMapperException(type.getSimpleName() + " was not found")));
        });
        if (wasComputed[0])
            completableFuture = completableFuture.thenApply(t1 -> {
                externalsHandler.populateExternals(t1);
                return t1;
            });

        else logger.info("{} with id {} obtained from IdentityMap", type.getSimpleName(), k);

        return completableFuture
                .thenApply(Optional::of)
                .exceptionally(throwable -> {
                    logger.warn("Removing CompletableFuture of {} from identityMap due to {}", type.getSimpleName(), throwable.getMessage());
                    identityMap.remove(k);
                    return Optional.empty();
                });
    }

    @Override
    public CompletableFuture<List<T>> findAll(UnitOfWork unit) {
        return find(() -> mapper.findAll(unit));
    }

    @Override
    public CompletableFuture<List<T>> findAll(UnitOfWork unit, int page, int numberOfItems) {
        return find(() -> mapper.findAll(unit, page, numberOfItems));
    }

    @Override
    public CompletableFuture<Void> create(UnitOfWork unit, T t) {
        t.markNew(unit);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> createAll(UnitOfWork unit, Iterable<T> t) {
        t.forEach(t1 -> t1.markNew(unit));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> update(UnitOfWork unit, T t) {
        CompletableFuture<T> future = identityMap.computeIfPresent(t.getIdentityKey(), (k, tCompletableFuture) ->
                tCompletableFuture.thenApply(t2 -> {
                    t2.markToBeDirty(unit);
                    return t2;
                })
        );

        if (future != null) {
            return future.thenApply(t1 -> {
                t.markDirty(unit);
                return null;
            });
        } else {
            return findById(unit, t.getIdentityKey())
                    .thenApply(t1 -> t1.orElseThrow(() -> new DataMapperException(type.getSimpleName() + " not found")))
                    .thenApply(t1 -> {
                        t1.markToBeDirty(unit);
                        t.markDirty(unit);
                        return null;
                    });
        }

    }

    @Override
    public CompletableFuture<Void> updateAll(UnitOfWork unit, Iterable<T> t) {
        List<CompletableFuture<T>> completableFutures = new ArrayList<>();
        t.forEach(t1 -> {
            CompletableFuture<T> future = identityMap.computeIfPresent(t1.getIdentityKey(), (k, tCompletableFuture) ->
                    tCompletableFuture.thenApply(t2 -> {
                        t2.markToBeDirty(unit);
                        return t2;
                    }));

            if (future != null) {
                completableFutures.add(future);
            } else {
                CompletableFuture<T> objectCompletableFuture = findById(unit, t1.getIdentityKey())
                        .thenApply(t2 -> t2.orElseThrow(() -> new DataMapperException(type.getSimpleName() + " not found")))
                        .thenApply(t2 -> {
                            t2.markToBeDirty(unit);
                            return t2;
                        });
                completableFutures.add(objectCompletableFuture);
            }
            t1.markDirty(unit);
        });
        return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]));
    }

    @Override
    public CompletableFuture<Void> deleteById(UnitOfWork unit, K k) {
        CompletableFuture<T> future = identityMap.computeIfPresent(k, (key, tCompletableFuture) -> tCompletableFuture.thenApply(t -> {
            t.markRemoved(unit);
            return t;
        }));

        return future != null
                ? future.thenApply(t -> null)
                : findById(unit, k)
                .thenApply(t -> {
                    T t1 = t.orElseThrow(() -> new DataMapperException("Object to delete was not found"));
                    t1.markRemoved(unit);
                    return null;
                });
    }

    @Override
    public CompletableFuture<Void> delete(UnitOfWork unit, T t) {
        t.markRemoved(unit);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteAll(UnitOfWork unit, Iterable<K> keys) {
        List<CompletableFuture> completableFutures = new ArrayList<>();
        keys.forEach(k -> {
            CompletableFuture<T> future = identityMap.computeIfPresent(k, (key, tCompletableFuture) -> tCompletableFuture
                    .thenApply(t -> {
                        t.markRemoved(unit);
                        return t;
                    })
            );

            completableFutures.add(future == null ?
                    findById(unit, k).thenAccept(t -> t.ifPresent(t1 -> t1.markRemoved(unit))) :
                    future);
        });

        return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]));
    }

    private CompletableFuture<List<T>> find(Supplier<CompletableFuture<List<T>>> supplier){
        return supplier.get()
                .thenApply(this::processNewObjects)
                .thenCompose(CollectionUtils::listToCompletableFuture);
    }

    public void invalidate(K identityKey) {
        identityMap.remove(identityKey);
    }

    public void validate(K identityKey, T t) {
        identityMap.compute(identityKey,
                (k, tCompletableFuture) -> tCompletableFuture == null
                        ? CompletableFuture.completedFuture(t)
                        : tCompletableFuture.thenApply(t1 -> getHighestVersionT(t, t1))
        );
    }

    private T getHighestVersionT(T t, T t1) {
        return t.getVersion() > t1.getVersion() ? t : t1;
    }

    private List<CompletableFuture<T>> processNewObjects(List<T> tList) {
        return tList
                .stream()
                .map(t -> {
                    boolean[] wasComputed = {false};
                    CompletableFuture<T> future = identityMap.compute(t.getIdentityKey(), (k, tCompletableFuture) -> computeNewValue(wasComputed, t, tCompletableFuture));
                    if (wasComputed[0])
                        return future.thenApply(t1 -> {
                            externalsHandler.populateExternals(t1);
                            return t1;
                        });
                    return future;
                })
                .collect(Collectors.toList());
    }

    private CompletableFuture<T> computeNewValue(boolean[] wasComputed, T newT, CompletableFuture<T> actualFuture) {
        CompletableFuture<T> newFuture = CompletableFuture.completedFuture(newT);


        if (actualFuture == null) {
            wasComputed[0] = true;
            return newFuture;
        }
        return actualFuture.thenApply(t -> {
            if(comparator.compare(t, newT) < 0){
                wasComputed[0] = true;
                return newT;
            }
            return t;
        });
        //TODO remove join
//        T actualT = actualFuture.join();
//        if (comparator.compare(actualT, newT) < 0) {
//            wasComputed[0] = true;
//            return newFuture;
//        }
//        return actualFuture;
    }

    Class<K> getKeyType() {
        return keyType;
    }
}
