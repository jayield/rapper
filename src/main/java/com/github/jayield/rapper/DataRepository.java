package com.github.jayield.rapper;

import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.exceptions.UnitOfWorkException;
import com.github.jayield.rapper.utils.*;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    private UnitOfWork checkUnitOfWork() {
        try {
            return UnitOfWork.getCurrent();
        } catch (UnitOfWorkException e) {
            ConnectionManager connectionManager = getConnectionManager(DBsPath.DEFAULTDB);
            SqlSupplier<Connection> connectionSupplier = connectionManager::getConnection;
            UnitOfWork.newCurrent(connectionSupplier.wrap());
            return UnitOfWork.getCurrent();
        }
    }

    @Override
    public <R> CompletableFuture<List<T>> findWhere(Pair<String, R>... values) {
        return find(() -> mapper.findWhere(values));
    }

    @Override
    public <R> CompletableFuture<List<T>> findWhere(int page, Pair<String, R>... values) {
        return find(() -> mapper.findWhere(page, values));
    }

    @Override
    public CompletableFuture<Optional<T>> findById(K k) {
        UnitOfWork current = checkUnitOfWork();

        boolean[] wasComputed = {false};
        CompletableFuture<T> completableFuture = identityMap.computeIfAbsent(
                k,
                k1 -> {
                    wasComputed[0] = true;
                    return mapper
                            .findById(k)
                            .thenApply(t -> t.orElseThrow(() -> new DataMapperException(type.getSimpleName() + " was not found")));
                }
        );
        if(wasComputed[0]) populateExternals(current, completableFuture);

        return completableFuture
                .thenApply(Optional::of)
                .exceptionally(throwable -> {
                    logger.debug("Removing CompletableFuture from identityMap.\nReason: {}", throwable.getMessage());
                    identityMap.remove(k);
                    return Optional.empty();
                });
    }

    @Override
    public CompletableFuture<List<T>> findAll() {
        return find(mapper::findAll);
    }

    @Override
    public CompletableFuture<List<T>> findAll(int page) {
        return find(() -> mapper.findAll(page));
    }

    @Override
    public CompletableFuture<Boolean> create(T t) {
        UnitOfWork unitOfWork = checkUnitOfWork();
        t.markNew();
        return unitOfWork.commit();
    }

    @Override
    public CompletableFuture<Boolean> createAll(Iterable<T> t) {
        UnitOfWork unitOfWork = checkUnitOfWork();
        t.forEach(DomainObject::markNew);
        return unitOfWork.commit();
    }

    @Override
    public CompletableFuture<Boolean> update(T t) {
        UnitOfWork unitOfWork = checkUnitOfWork();

        CompletableFuture<T> future = identityMap.computeIfPresent(t.getIdentityKey(), (k, tCompletableFuture) ->
                tCompletableFuture.thenApply(t2 -> {
                    t2.markToBeDirty();
                    return t2;
                }));

        if (future != null) {
            return future.thenCompose(t1 -> {
                t.markDirty();
                return unitOfWork.commit();
            });
        } else {
            return findById(t.getIdentityKey())
                    .thenApply(t1 -> t1.orElseThrow(() -> new DataMapperException(type.getSimpleName() + " not found")))
                    .thenCompose(t1 -> {
                        t1.markToBeDirty();
                        t.markDirty();
                        return unitOfWork.commit();
                    });
        }
    }

    @Override
    public CompletableFuture<Boolean> updateAll(Iterable<T> t) {
        UnitOfWork unitOfWork = checkUnitOfWork();
        List<CompletableFuture<T>> completableFutures = new ArrayList<>();
        t.forEach(t1 -> {
            CompletableFuture<T> future = identityMap.computeIfPresent(t1.getIdentityKey(), (k, tCompletableFuture) ->
                    tCompletableFuture.thenApply(t2 -> {
                        t2.markToBeDirty();
                        return t2;
                    }));

            if (future != null)
                completableFutures.add(future);
            else {
                CompletableFuture<T> objectCompletableFuture = findById(t1.getIdentityKey())
                        .thenApply(t2 -> t2.orElseThrow(() -> new DataMapperException(type.getSimpleName() + " not found")))
                        .thenApply(t2 -> {
                            t2.markToBeDirty();
                            return t2;
                        });
                completableFutures.add(objectCompletableFuture);
            }
            t1.markDirty();
        });
        return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]))
                .thenCompose(aVoid -> unitOfWork.commit());
    }

    @Override
    public CompletableFuture<Boolean> deleteById(K k) {
        UnitOfWork unitOfWork = checkUnitOfWork();
        CompletableFuture<T> future = identityMap.computeIfPresent(k, (key, tCompletableFuture) -> tCompletableFuture.thenApply(t -> {
            t.markRemoved();
            return t;
        }));

        return future != null
                ? future.thenCompose(t -> unitOfWork.commit())
                : findById(k)
                .thenCompose(t -> {
                    T t1 = t.orElseThrow(() -> new DataMapperException("Object to delete was not found"));
                    t1.markRemoved();
                    return unitOfWork.commit();
                });
    }

    @Override
    public CompletableFuture<Boolean> delete(T t) {
        UnitOfWork unitOfWork = checkUnitOfWork();
        t.markRemoved();
        return unitOfWork.commit();
    }

    @Override
    public CompletableFuture<Boolean> deleteAll(Iterable<K> keys) {
        UnitOfWork unitOfWork = checkUnitOfWork();
        List<CompletableFuture> completableFutures = new ArrayList<>();
        keys.forEach(k -> {
            CompletableFuture<T> future = identityMap.computeIfPresent(k, (key, tCompletableFuture) -> tCompletableFuture.thenApply(t -> {
                UnitOfWork.setCurrent(unitOfWork);
                t.markRemoved();
                return t;
            }));

            completableFutures.add(future == null
                    ?
                    findById(k)
                            .thenAccept(t -> t.ifPresent(t1 -> {
                                UnitOfWork.setCurrent(unitOfWork);
                                t1.markRemoved();
                            }))
                    :
                    future);
        });

        return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]))
                .thenCompose(aVoid -> unitOfWork.commit());
    }


    private CompletableFuture<List<T>> find(Supplier<CompletableFuture<List<T>>> supplier){
        UnitOfWork current = checkUnitOfWork();
        return supplier.get()
                .thenApply(Collection::stream)
                .thenApply(tStream -> processNewObjects(current, tStream))
                .thenApply(completableFutureStream -> completableFutureStream.collect(Collectors.toList()))
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

    private Stream<CompletableFuture<T>> processNewObjects(UnitOfWork current, Stream<T> tStream) {
        return tStream.map(t -> {
            boolean[] wasComputed = {false};
            CompletableFuture<T> future = identityMap.compute(t.getIdentityKey(), (k, tCompletableFuture) -> computeNewValue(wasComputed, t, tCompletableFuture));
            if(wasComputed[0]) populateExternals(current, future);
            return future;
        });
    }

    private void populateExternals(UnitOfWork current, CompletableFuture<T> future) {
        future.thenApply(t1 -> {
            UnitOfWork.setCurrent(current);
            externalsHandler.populateExternals(t1);
            return t1;
        });
    }

    private CompletableFuture<T> computeNewValue(boolean[] wasComputed, T newT, CompletableFuture<T> actualFuture) {
        CompletableFuture<T> newFuture = CompletableFuture.completedFuture(newT);

        if (actualFuture == null) {
            wasComputed[0] = true;
            return newFuture;
        }
        T actualT = actualFuture.join();
        if (comparator.compare(actualT, newT) < 0) {
            wasComputed[0] = true;
            return newFuture;
        }
        return actualFuture;
    }

    Class<K> getKeyType() {
        return keyType;
    }
}
