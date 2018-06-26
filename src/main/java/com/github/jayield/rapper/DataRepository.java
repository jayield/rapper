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

    private <R> R checkUnitOfWork(BiFunction<UnitOfWork, Boolean, R> action) {
        try {
            UnitOfWork current = UnitOfWork.getCurrent();
            return action.apply(current, false);
        } catch (UnitOfWorkException e) {
            logger.info("new unit");
            ConnectionManager connectionManager = getConnectionManager(DBsPath.DEFAULTDB);
            SqlSupplier<CompletableFuture<SQLConnection>> connectionSupplier = connectionManager::getConnection;

            UnitOfWork unitOfWork = UnitOfWork.newCurrent(connectionSupplier.wrap());
            R r = action.apply(unitOfWork, true);
            UnitOfWork.removeCurrent();
            return r;
        }
    }

    @Override
    public <R> CompletableFuture<Long> getNumberOfEntries(Pair<String, R>... values) {
        return checkUnitOfWork((unitOfWork, isNewUnit) ->  mapper.getNumberOfEntries(values));
    }

    @Override
    public CompletableFuture<Long> getNumberOfEntries() {
        return checkUnitOfWork((unitOfWork, isNewUnit) -> mapper.getNumberOfEntries());
    }

    @Override
    public <R> CompletableFuture<List<T>> findWhere(Pair<String, R>... values) {
        return find(() -> mapper.findWhere(values));
    }

    @Override
    public <R> CompletableFuture<List<T>> findWhere(int page, int numberOfitems, Pair<String, R>... values) {
        return find(() -> mapper.findWhere(page, numberOfitems, values));
    }

    @Override
    public CompletableFuture<Optional<T>> findById(K k) {
        return checkUnitOfWork((current, isNewUnit) -> {
            boolean[] wasComputed = {false};
            System.out.println("k " + k + " identitymap " + identityMap);
            CompletableFuture<T> completableFuture = identityMap.computeIfAbsent(
                    k,
                    k1 -> {
                        wasComputed[0] = true;
                        return mapper
                                .findById(k)
                                .thenApply(t -> t.orElseThrow(() -> new DataMapperException(type.getSimpleName() + " was not found")));
                    }
            );
            if (wasComputed[0])
                completableFuture = completableFuture.thenApply(t1 -> UnitOfWork.executeActionWithNewUnit(() -> {
                            externalsHandler.populateExternals(t1);
                            return t1;
                        })
                );

            else logger.info("{} with id {} obtained from IdentityMap", type.getSimpleName(), k);

            return completableFuture
                    .thenCompose(t -> isNewUnit ? current.commit().thenApply(aVoid -> Optional.of(t)) : CompletableFuture.completedFuture(Optional.of(t)))
                    .exceptionally(throwable -> {
                        logger.warn("Removing CompletableFuture of {} from identityMap due to {}", type.getSimpleName(), throwable.getMessage());
                        identityMap.remove(k);
                        return Optional.empty();
                    });
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
        return checkUnitOfWork((unitOfWork, isNewUnit) -> {
            t.markNew();
            return checkCommit(unitOfWork, isNewUnit);
        });
    }

    @Override
    public CompletableFuture<Void> createAll(Iterable<T> t) {
        return checkUnitOfWork((unitOfWork, isNewUnit) -> {
            t.forEach(DomainObject::markNew);
            return checkCommit(unitOfWork, isNewUnit);
        });
    }

    @Override
    public CompletableFuture<Void> update(T t) {
        return checkUnitOfWork((unitOfWork, isNewUnit) -> {
            CompletableFuture<T> future = identityMap.computeIfPresent(t.getIdentityKey(), (k, tCompletableFuture) ->
                    tCompletableFuture.thenApply(t2 -> UnitOfWork.executeActionWithinUnit(unitOfWork, () -> {
                        t2.markToBeDirty();
                        return t2;
                    }))
            );

            if (future != null) {
                return future.thenCompose(t1 -> UnitOfWork.executeActionWithinUnit(unitOfWork, () -> {
                    t.markDirty();
                    return checkCommit(unitOfWork, isNewUnit);
                }));
            } else {
                return findById(t.getIdentityKey())
                        .thenApply(t1 -> t1.orElseThrow(() -> new DataMapperException(type.getSimpleName() + " not found")))
                        .thenCompose(t1 -> UnitOfWork.executeActionWithinUnit(unitOfWork, () -> {
                            t1.markToBeDirty();
                            t.markDirty();
                            return checkCommit(unitOfWork, isNewUnit);
                        }));
            }
        });

    }

    @Override
    public CompletableFuture<Void> updateAll(Iterable<T> t) {
        return checkUnitOfWork((unitOfWork, isNewUnit) -> {
            assert(isNewUnit);
            List<CompletableFuture<T>> completableFutures = new ArrayList<>();
            t.forEach(t1 -> {
                CompletableFuture<T> future = identityMap.computeIfPresent(t1.getIdentityKey(), (k, tCompletableFuture) ->
                        tCompletableFuture.thenApply(t2 -> {
                            t2.markToBeDirty();
                            return t2;
                        }));

                if (future != null) {
                    completableFutures.add(future);
                }
                else {
                    CompletableFuture<T> objectCompletableFuture = findById(t1.getIdentityKey())
                            .thenApply(t2 -> t2.orElseThrow(() -> new DataMapperException(type.getSimpleName() + " not found")))
                            .thenApply(t2 -> UnitOfWork.executeActionWithinUnit(unitOfWork, () -> {
                                t2.markToBeDirty();
                                return t2;
                            }));
                    completableFutures.add(objectCompletableFuture);
                }
                t1.markDirty();
            });
            return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]))
                    .thenCompose(aVoid -> checkCommit(unitOfWork, isNewUnit));
        });
    }

    @Override
    public CompletableFuture<Void> deleteById(K k) {
        return checkUnitOfWork((unitOfWork, isNewUnit) -> {
            CompletableFuture<T> future = identityMap.computeIfPresent(k, (key, tCompletableFuture) -> tCompletableFuture.thenApply(t -> {
                t.markRemoved();
                return t;
            }));

            return future != null
                    ? future.thenCompose(t -> checkCommit(unitOfWork, isNewUnit))
                    : findById(k)
                    .thenCompose(t -> UnitOfWork.executeActionWithinUnit(unitOfWork, () -> {
                        T t1 = t.orElseThrow(() -> new DataMapperException("Object to delete was not found"));
                        t1.markRemoved();
                        return checkCommit(unitOfWork, isNewUnit);
                    }));
        });
    }

    @Override
    public CompletableFuture<Void> delete(T t) {
        return checkUnitOfWork((unitOfWork, isNewUnit) -> {
            t.markRemoved();
            return checkCommit(unitOfWork, isNewUnit);
        });
    }

    @Override
    public CompletableFuture<Void> deleteAll(Iterable<K> keys) {
        return checkUnitOfWork((unitOfWork, isNewUnit) -> {
            List<CompletableFuture> completableFutures = new ArrayList<>();
            keys.forEach(k -> {
                CompletableFuture<T> future = identityMap.computeIfPresent(k, (key, tCompletableFuture) -> tCompletableFuture
                        .thenApply(t -> UnitOfWork.executeActionWithinUnit(unitOfWork, () -> {
                            t.markRemoved();
                            return t;
                        }))
                );

                completableFutures.add(future == null
                        ?
                        findById(k)
                                .thenAccept(t -> t.ifPresent(t1 -> UnitOfWork.executeActionWithinUnit(unitOfWork, () -> {
                                            t1.markRemoved();
                                            return null;
                                        }))
                                )
                        :
                        future);
            });

            return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]))
                    .thenCompose(aVoid -> checkCommit(unitOfWork, isNewUnit));
        });
    }

    private CompletableFuture<Void> checkCommit(UnitOfWork unitOfWork, Boolean isNewUnit) {
        return isNewUnit ? unitOfWork.commit() : CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<List<T>> find(Supplier<CompletableFuture<List<T>>> supplier){
        return checkUnitOfWork((unitOfWork, isNewUnit) -> supplier.get()
                .thenApply(ts -> processNewObjects(ts))
                .thenCompose(CollectionUtils::listToCompletableFuture)
                .thenCompose(ts -> isNewUnit ? unitOfWork.commit().thenApply(aVoid -> ts) : CompletableFuture.completedFuture(ts))
        );
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
                        return future.thenApply(t1 -> UnitOfWork.executeActionWithNewUnit(() ->  {
                            externalsHandler.populateExternals(t1);
                            return t1;
                        }));
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
