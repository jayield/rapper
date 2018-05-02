package com.github.jayield.rapper;

import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.exceptions.UnitOfWorkException;
import javafx.util.Pair;
import com.github.jayield.rapper.utils.ConnectionManager;
import com.github.jayield.rapper.utils.DBsPath;
import com.github.jayield.rapper.utils.SqlSupplier;
import com.github.jayield.rapper.utils.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.github.jayield.rapper.utils.ConnectionManager.*;

public class DataRepository<T extends DomainObject<K>, K> implements Mapper<T, K> {

    private static final Logger logger = LoggerFactory.getLogger(DataRepository.class);
    private final ConcurrentMap<K, CompletableFuture<T>> identityMap = new ConcurrentHashMap<>();
    private final Mapper<T, K> mapper;    //Used to communicate with the DB

    public DataRepository(Mapper<T, K> mapper) {
        this.mapper = mapper;
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
        checkUnitOfWork();
        return mapper.findWhere(values);
    }

    @Override
    public CompletableFuture<Optional<T>> findById(K k) {
        checkUnitOfWork();

        CompletableFuture<T> completableFuture = identityMap.computeIfAbsent(k, k1 -> mapper.findById(k).thenApply(t -> t.orElseThrow(() ->
                new DataMapperException("Object was not found"))));

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
        checkUnitOfWork();
        CompletableFuture<List<T>> completableFuture = mapper.findAll();
        completableFuture.thenAccept(list -> list.forEach(this::putOrReplace));
        return completableFuture;
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

        if (future != null)
            return future.thenCompose(t1 -> {
                t.markDirty();
                return unitOfWork.commit();
            });
        else {
            t.markDirty();
            return unitOfWork.commit();
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

    private void putOrReplace(T item) {
        K key = item.getIdentityKey();
        identityMap.compute(key, (k, v) -> CompletableFuture.completedFuture(item));
    }

    public void invalidate(K identityKey) {
        identityMap.remove(identityKey);
    }

    public void validate(K identityKey, T t) {
        identityMap.put(identityKey, CompletableFuture.completedFuture(t));
    }

    public boolean tryReplace(T obj) {
        long target = System.currentTimeMillis() + (long) 2000;
        long remaining = target - System.currentTimeMillis();

        while (remaining >= 0) {
            CompletableFuture<T> observedObj = identityMap.putIfAbsent(obj.getIdentityKey(), CompletableFuture.completedFuture(obj));
            if (observedObj == null) return true;
            if (observedObj.join().getVersion() < obj.getVersion()) {
                if (identityMap.replace(obj.getIdentityKey(), observedObj, CompletableFuture.completedFuture(obj)))
                    return true;
            } else
                return true; //TODO should we log a message saying a newer version is already present in the identityMap?
            remaining = target - System.currentTimeMillis();
            Thread.yield();
        }
        return false;
    }
}
