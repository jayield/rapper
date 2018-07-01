package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.exceptions.DataMapperException;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.TransactionIsolation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;

import static com.github.jayield.rapper.utils.MapperRegistry.getRepository;

public class UnitOfWork {
    private static final String UNSUCCESSFUL_COMMIT_MESSAGE = "{} - Rolling back changes due to {}";
    private static final Logger logger = LoggerFactory.getLogger(UnitOfWork.class);
    public static final AtomicInteger numberOfOpenConnections = new AtomicInteger(0);

    /**
     * Private for each transaction
     */
    private CompletableFuture<SQLConnection> connection = null;
    private final Supplier<CompletableFuture<SQLConnection>> connectionSupplier;

    //Multiple Threads may be accessing the Queue, so it must be a ConcurrentLinkedQueue
    private final Queue<DomainObject> newObjects = new ConcurrentLinkedQueue<>();
    private final Queue<DomainObject> clonedObjects = new ConcurrentLinkedQueue<>();
    private final Queue<DomainObject> dirtyObjects = new ConcurrentLinkedQueue<>();
    private final Queue<DomainObject> removedObjects = new ConcurrentLinkedQueue<>();
    private final CreateHelper createHelper = new CreateHelper(newObjects);
    private final UpdateHelper updateHelper = new UpdateHelper(dirtyObjects, clonedObjects, removedObjects);
    private final DeleteHelper deleteHelper = new DeleteHelper(removedObjects);

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
            String string = Thread.currentThread().getStackTrace()[2].toString();
            logger.info("{} - New connection opened from {}", this.hashCode(), string);
        }
        return connection;
    }

    private CompletableFuture<Void> closeConnection(){
        numberOfOpenConnections.decrementAndGet();
        String string = Thread.currentThread().getStackTrace()[2].toString();
        logger.info("{} - Closing connection from {}", this.hashCode(), string);
        return connection.thenAccept(SQLConnection::close)
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
        if (newObjects.contains(obj)) throw new AssertionError();
        newObjects.add(obj);
    }

    /**
     * It will be created a clone of obj, in case a rollback is done, we have a way to go back as it was before
     * @param obj DomainObject to be cloned
     */
    public void registerClone(DomainObject obj) {
        if (obj.getIdentityKey() == null) throw new AssertionError();
        if (removedObjects.contains(obj)) throw new AssertionError();
        if(!clonedObjects.contains(obj) && !newObjects.contains(obj))
            clonedObjects.add(obj);
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
            if (newObjects.isEmpty() && clonedObjects.isEmpty() && dirtyObjects.isEmpty() && removedObjects.isEmpty()) {
                CompletableFuture<Void> toRet = CompletableFuture.completedFuture(null);
                if (connection != null) {
                    toRet = connection.thenCompose(con -> SQLUtils.callbackToPromise(con::commit))
                            .thenCompose(v -> closeConnection())
                            .thenAccept(v -> logger.info("{} - Changes have been committed", this.hashCode()));
                }
                return toRet;
            } else {
                List<CompletableFuture<Void>> completableFutures = iterateMultipleLists(abstractCommitHelper -> abstractCommitHelper.commitNext(this));
                createHelper.reset();
                updateHelper.reset();
                deleteHelper.reset();

                return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]))
                        .thenCompose(aVoid -> proceedCommit())
                        .thenCompose(v ->
                                connection.thenCompose(con ->
                                        SQLUtils.callbackToPromise(con::commit)
                                                .thenAccept(v2 -> logger.info("{} - Changes have been committed", this.hashCode()))))
                        .thenCompose(v -> closeConnection())
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
            }
        } catch (DataMapperException e){
            logger.info(UNSUCCESSFUL_COMMIT_MESSAGE, this.hashCode(), e.getMessage());
            return rollback().thenAccept(aVoid -> { throw e; });
        }
    }

    private CompletableFuture<Void> proceedCommit() {
        List<CompletableFuture<Void>> futures = iterateMultipleLists(AbstractCommitHelper::identityMapUpdateNext);
        createHelper.reset();
        updateHelper.reset();
        deleteHelper.reset();
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
    }

    public List<CompletableFuture<Void>> iterateMultipleLists(Function<AbstractCommitHelper, CompletableFuture<Void>> function) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        CompletableFuture<Void> createFuture;
        CompletableFuture<Void> updateFuture;
        CompletableFuture<Void> deleteFuture;
        do {
            createFuture = function.apply(createHelper);
            updateFuture = function.apply(updateHelper);
            deleteFuture = function.apply(deleteHelper);

            if (createFuture != null) futures.add(createFuture);
            if (updateFuture != null) futures.add(updateFuture);
            if (deleteFuture != null) futures.add(deleteFuture);
        } while (createFuture != null || updateFuture != null || deleteFuture != null);
        return futures;
    }

    /**
     * Removes the objects from the newObjects from the IdentityMap
     * Puts the objects in removedObjects into the IdentityMap
     * The objects in dirtyObjects need to go back as before
     */
    public CompletableFuture<Void> rollback() {
        try {
            //System.out.println("connection in rollbak " + connection);
            if(connection != null) {
                return connection.thenCompose(con -> SQLUtils.callbackToPromise(con::rollback))
                        .thenCompose(v -> closeConnection());
            }

            newObjects.forEach(domainObject -> getRepository(domainObject.getClass()).invalidate(domainObject.getIdentityKey()));

            for (DomainObject obj : dirtyObjects) {
                clonedObjects.stream()
                        .filter(domainObject -> domainObject.getIdentityKey().equals(obj.getIdentityKey()))
                        .findFirst()
                        .ifPresent(
                                clone -> getRepository(obj.getClass()).validate(clone.getIdentityKey(), clone)
                        );
            }

            removedObjects.stream()
                    .filter(obj -> !dirtyObjects.contains(obj))
                    .forEach(obj -> getRepository(obj.getClass()).validate(obj.getIdentityKey(), obj));

            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            throw new DataMapperException(e);
        }
    }
}
