package com.github.jayield.rapper.unitofwork;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.mapper.externals.ExternalsHandler;
import com.github.jayield.rapper.utils.Pair;
import com.github.jayield.rapper.utils.SqlUtils;
import com.github.jayield.rapper.connections.ConnectionManager;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.TransactionIsolation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;
import java.util.stream.Collectors;

public class UnitOfWork {
    private static final String UNSUCCESSFUL_COMMIT_MESSAGE = "{} - Rolling back changes due to {}";
    private static final Logger logger = LoggerFactory.getLogger(UnitOfWork.class);
    public static final AtomicInteger numberOfOpenConnections = new AtomicInteger(0);
    public static final ConcurrentMap<SQLConnection, StackTraceElement[]> connectionsMap = new ConcurrentHashMap<>();

    private CompletableFuture<SQLConnection> connection = null;
    private final Supplier<CompletableFuture<SQLConnection>> connectionSupplier;
    private final ConcurrentMap<Class<? extends DomainObject>, ConcurrentHashMap<Object, CompletableFuture<? extends DomainObject>>> identityMap = new ConcurrentHashMap<>();

    //Multiple Threads may be accessing the Queue, so it must be a ConcurrentLinkedQueue
    private final Queue<DomainObject> newObjects = new ConcurrentLinkedQueue<>();
    private final Queue<DomainObject> dirtyObjects = new ConcurrentLinkedQueue<>();
    private final Queue<DomainObject> removedObjects = new ConcurrentLinkedQueue<>();
    private final CreateHelper createHelper = new CreateHelper(this, newObjects);
    private final UpdateHelper updateHelper = new UpdateHelper(this, dirtyObjects, removedObjects);
    private final DeleteHelper deleteHelper = new DeleteHelper(this, removedObjects, dirtyObjects);

    public UnitOfWork(Supplier<CompletableFuture<SQLConnection>> connectionSupplier){
        this.connectionSupplier = connectionSupplier;
    }

    public UnitOfWork() {
        this.connectionSupplier = ConnectionManager.getConnectionManager()::getConnection;
    }

    public UnitOfWork(TransactionIsolation isolation) {
        this.connectionSupplier = () -> ConnectionManager.getConnectionManager().getConnection(isolation.getType());
    }

    public CompletableFuture<SQLConnection> getConnection() {
        if(connection == null) {
            connection = connectionSupplier.get();
            numberOfOpenConnections.incrementAndGet();
            StackTraceElement[] stack = Thread.currentThread().getStackTrace();
            connection = connection.thenApply(con -> {
                connectionsMap.put(con, stack);
                return con;
            });
            String string = Thread.currentThread().getStackTrace()[2].toString();
            logger.info("{} - New connection opened from {}", this.hashCode(), string);
        }
        return connection;
    }

    private CompletableFuture<Void> closeConnection(){
        numberOfOpenConnections.decrementAndGet();
        String string = Thread.currentThread().getStackTrace()[2].toString();
        logger.info("{} - Closing connection from {}", this.hashCode(), string);
        return connection.thenApply(con -> {
            connectionsMap.remove(con);
            return con;
        })
                .thenAccept(SQLConnection::close)
                .thenAccept(v -> connection = null);
    }

    /**
     * Adds the obj to the newObjects List and to the IdentityMap
     * @param obj
     */
    public void registerNew(DomainObject obj) {
        if (obj.getIdentityKey() == null) throw new AssertionError();
        if (dirtyObjects.contains(obj)) throw new AssertionError();
        if (removedObjects.contains(obj)) throw new AssertionError();
        if (newObjects.contains(obj)) return;
        newObjects.add(obj);
    }

    /**
     * Tags the object to be updated on the DB
     * @param obj
     */
    public void registerDirty(DomainObject obj){
        if (obj.getIdentityKey() == null) throw new AssertionError();
        if (removedObjects.contains(obj)) throw new AssertionError();
        if(!dirtyObjects.contains(obj) && !newObjects.contains(obj))
            dirtyObjects.add(obj);
    }

    /**
     * Removes the obj from newObjects and/or dirtyObjects and from the IdentityMap
     * @param obj
     */
    public void registerRemoved(DomainObject obj){
        if (obj.getIdentityKey() == null) throw new AssertionError();
        if(newObjects.remove(obj)) return;
        dirtyObjects.remove(obj);
        if(!removedObjects.contains(obj))
            removedObjects.add(obj);
    }

    public CompletableFuture<Void> commit() {
        try {
            if (newObjects.isEmpty() && dirtyObjects.isEmpty() && removedObjects.isEmpty()) {
                CompletableFuture<Void> toRet = CompletableFuture.completedFuture(null);
                if (connection != null) {
                    toRet = connection.thenCompose(con -> SqlUtils.callbackToPromise(con::commit))
                            .thenCompose(v -> closeConnection())
                            .thenAccept(v -> logger.info("{} - Changes have been committed", this.hashCode()));
                }
                return toRet;
            } else if (connection != null){
                return connection.thenCompose(con -> SqlUtils.callbackToPromise(con::commit)
                                .thenAccept(v2 -> logger.info("{} - Changes have been committed", this.hashCode())))
                        .thenCompose(v -> {
                            iterateMultipleLists(AbstractCommitHelper::identityMapUpdateNext);
                            return closeConnection();
                        })
                        .handleAsync((aVoid, throwable) -> {
                            if (throwable != null) {
                                logger.info(UNSUCCESSFUL_COMMIT_MESSAGE, this.hashCode(), throwable.getMessage());
                                return rollback().thenAccept(aVoid1 -> {
                                    throw new DataMapperException(throwable);
                                });
                            }
                            return CompletableFuture.<Void>completedFuture(null);
                        })
                        .thenCompose(voidCompletableFuture -> voidCompletableFuture);
            } else {
                iterateMultipleLists(AbstractCommitHelper::identityMapUpdateNext);
                return CompletableFuture.completedFuture(null);
            }
        } catch (DataMapperException e){
            logger.info(UNSUCCESSFUL_COMMIT_MESSAGE, this.hashCode(), e.getMessage());
            return rollback().thenAccept(aVoid -> { throw e; });
        }
    }

    /**
     * Removes the objects from the newObjects from the IdentityMap
     * Puts the objects in removedObjects into the IdentityMap
     * The objects in dirtyObjects need to go back as before
     */
    public CompletableFuture<Void> rollback() {
        try {
            if(connection != null) {
                return connection.thenCompose(con -> SqlUtils.callbackToPromise(con::rollback))
                        .thenCompose(v -> closeConnection())
                        .thenAccept(aVoid -> iterateMultipleLists(AbstractCommitHelper::rollbackNext));
            }

            iterateMultipleLists(AbstractCommitHelper::rollbackNext);
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            throw new DataMapperException(e);
        }
    }

    public <N> List<N> iterateMultipleLists(Function<AbstractCommitHelper, N> function) {
        List<N> list = new ArrayList<>();

        N createFuture;
        N updateFuture;
        N deleteFuture;
        do {
            createFuture = function.apply(createHelper);
            updateFuture = function.apply(updateHelper);
            deleteFuture = function.apply(deleteHelper);

            if (createFuture != null) list.add(createFuture);
            if (updateFuture != null) list.add(updateFuture);
            if (deleteFuture != null) list.add(deleteFuture);
        } while (createFuture != null || updateFuture != null || deleteFuture != null);
        createHelper.reset();
        updateHelper.reset();
        deleteHelper.reset();
        return list;
    }

    public<T extends DomainObject<K>, K> void invalidate(Class<T> type, K identityKey) {
        getIdentityMap(type).remove(identityKey);
    }

    public<T extends DomainObject<K>, K> void validate(K identityKey, T t) {
        getIdentityMap(t.getClass()).compute(identityKey,
                (k, tCompletableFuture) -> tCompletableFuture == null
                        ? CompletableFuture.completedFuture(t)
                        : tCompletableFuture.thenApply(t1 -> getHighestVersionT(t, t1))
        );
    }

    private<T extends DomainObject<K>, K> T getHighestVersionT(T t, T t1) {
        return t.getVersion() > t1.getVersion() ? t : t1;
    }

    public<T extends DomainObject<K>, K> List<CompletableFuture<T>> processNewObjects(Class<T> type, List<T> tList, Comparator<T> comparator) {
        return tList.stream()
                .map(t -> processNewObject(type, t, comparator))
                .collect(Collectors.toList());
    }

    public  <T extends DomainObject<K>, K> CompletableFuture<T> processNewObject(Class<T> type, T t, Comparator<T> comparator) {
        return getIdentityMap(type)
                .compute(t.getIdentityKey(), (k, tCompletableFuture) ->
                        Optional.ofNullable(tCompletableFuture)
                                .map(completableFuture -> completableFuture.thenApply(t1 -> (T) t1))
                                .map(completableFuture -> completableFuture.thenApply(t1 -> {
                                    if (comparator.compare(t1, t) < 0) return t;
                                    return t1;
                                }))
                                .orElse(CompletableFuture.completedFuture(t))
                )
                .thenApply(obj -> (T) obj);
    }

    public ConcurrentMap<Object, CompletableFuture<? extends DomainObject>> getIdentityMap(Class<? extends DomainObject> klass) {
        return identityMap.computeIfAbsent(klass, aClass -> new ConcurrentHashMap<>());
    }
}
