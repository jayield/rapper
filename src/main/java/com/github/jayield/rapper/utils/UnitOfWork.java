package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.DataRepository;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.Mapper;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.exceptions.UnitOfWorkException;
import io.vertx.ext.sql.SQLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.github.jayield.rapper.utils.ConnectionManager.getConnectionManager;
import static com.github.jayield.rapper.utils.MapperRegistry.getRepository;

public class UnitOfWork {
    private static final String UNSUCCESSFUL_COMMIT_MESSAGE = "{} - Rolling back changes due to {}";
    private static final Logger logger = LoggerFactory.getLogger(UnitOfWork.class);

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

    protected UnitOfWork(Supplier<CompletableFuture<SQLConnection>> connectionSupplier){
        this.connectionSupplier = connectionSupplier;
    }

    public CompletableFuture<SQLConnection> getConnection() {
        if(connection == null) {
            connection = connectionSupplier.get();
            logger.info("{} - New connection opened", this.hashCode());
        }
        return connection;
    }

    private CompletableFuture<Void> closeConnection(){ //TODO remove join
        return connection.thenAccept(SQLConnection::close)
                .thenAccept(v -> connection = null); // Not sure of this join maybe close connection should return CF<Void>
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

    private static ThreadLocal<UnitOfWork> current = new ThreadLocal<>();

    /**
     * Each Thread will have its own UnitOfWork
     */
    public static UnitOfWork newCurrent(Supplier<CompletableFuture<SQLConnection>> supplier) {
        UnitOfWork uow = new UnitOfWork(supplier);
        setCurrent(uow);

        logger.info("New Unit of Work {}", uow.hashCode());

        return uow;
    }

    private static void setCurrent(UnitOfWork uow) {
        current.set(uow);
    }

    public static void removeCurrent(){
        UnitOfWork unitOfWork = current.get();
        if(unitOfWork!= null) logger.info("{} - Unit of Work removed", unitOfWork.hashCode());
        current.remove();
    }

    public static <R> R executeActionWithinUnit(UnitOfWork unitOfWork, Supplier<R> action) {
        try {
            UnitOfWork.getCurrent();
            return action.get();
        } catch (UnitOfWorkException e) {
            UnitOfWork.setCurrent(unitOfWork);
            R r = action.get();
            UnitOfWork.removeCurrent();
            return r;
        }
    }

    public static <R> R executeActionWithNewUnit(Supplier<R> action) {
        ConnectionManager connectionManager = getConnectionManager();
        SqlSupplier<CompletableFuture<SQLConnection>> connectionSupplier = connectionManager::getConnection;
        try {
            UnitOfWork current = UnitOfWork.getCurrent();

            UnitOfWork.newCurrent(connectionSupplier.wrap());
            R r = action.get();

            UnitOfWork.setCurrent(current);
            return r;
        } catch (UnitOfWorkException e) {
            UnitOfWork.newCurrent(connectionSupplier.wrap());
            R r = action.get();
            UnitOfWork.removeCurrent();
            return r;
        }
    }

    public static UnitOfWork getCurrent() {
        UnitOfWork unitOfWork = current.get();
        if(unitOfWork == null)
            throw new UnitOfWorkException("The Unit of Work you're trying to access is currently NULL. You must create or set it first.");
        return unitOfWork;
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
                Pair<List<DataRepository<? extends DomainObject<?>, ?>>,
                        List<CompletableFuture<Void>>> insertPair = executeFilteredBiFunctionInList(Mapper::create, newObjects, domainObject -> true);

                Pair<List<DataRepository<? extends DomainObject<?>, ?>>,
                        List<CompletableFuture<Void>>> updatePair = executeFilteredBiFunctionInList(Mapper::update, dirtyObjects, domainObject -> !removedObjects.contains(domainObject));

                Pair<List<DataRepository<? extends DomainObject<?>, ?>>,
                        List<CompletableFuture<Void>>> deletePair = executeFilteredBiFunctionInList(Mapper::delete, removedObjects, domainObject -> true);

                List<CompletableFuture<Void>> completableFutures = insertPair.getValue();
                completableFutures.addAll(updatePair.getValue());
                completableFutures.addAll(deletePair.getValue());

                return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]))
                        .thenAccept(aVoid -> proceedCommit(insertPair.getKey(), updatePair.getKey(), deletePair.getKey()))
                        .thenCompose(v ->
                                connection.thenCompose(con ->
                                        SQLUtils.callbackToPromise(con::commit)
                                                .thenAccept(v2 -> logger.info("{} - Changes have been committed", this.hashCode()))))
                        .thenCompose(v -> closeConnection())
                        .exceptionally(throwable -> {
                            logger.info(UNSUCCESSFUL_COMMIT_MESSAGE, this.hashCode(), throwable.getMessage());
                            rollback();
                            throw new DataMapperException(throwable);
                        });
            }
        } catch (DataMapperException e){
            logger.info(UNSUCCESSFUL_COMMIT_MESSAGE, this.hashCode(), e.getMessage());
            rollback();
            throw new DataMapperException(e);
        }
    }

    private void proceedCommit(List<DataRepository<? extends DomainObject<?>, ?>> insertRepos, List<DataRepository<? extends DomainObject<?>, ?>> updateRepos,
                               List<DataRepository<? extends DomainObject<?>, ?>> deleteRepos) {
//        try {
            //The different iterators will have the same size (eg. insertedReposIterator.size() == insertedObjectsIterator.size()
            Iterator<DataRepository<? extends DomainObject<?>, ?>> insertedReposIterator = insertRepos.iterator();
            Iterator<DataRepository<? extends DomainObject<?>, ?>> updatedReposIterator = updateRepos.iterator();
            Iterator<DataRepository<? extends DomainObject<?>, ?>> deletedReposIterator = deleteRepos.iterator();
            Iterator<DomainObject> insertedObjectsIterator = newObjects.iterator();
            Iterator<DomainObject> updatedObjectsIterator = dirtyObjects.iterator();
            Iterator<DomainObject> deletedObjectsIterator = removedObjects.iterator();

            //Will iterate through insert, update and delete iterators at the same time
            while (insertedReposIterator.hasNext() || updatedReposIterator.hasNext() || deletedReposIterator.hasNext()) {
                iterate(insertedReposIterator, insertedObjectsIterator, (repo, domainObject) -> {
                    repo.validate(domainObject.getIdentityKey(), domainObject);
                    MapperRegistry.getExternal(domainObject.getClass()).insertReferences(domainObject);
                });
                iterate(updatedReposIterator, updatedObjectsIterator, (repo, domainObject) -> {
                    repo.validate(domainObject.getIdentityKey(), domainObject);
                    DomainObject prevDomainObj = clonedObjects
                            .stream()
                            .filter(domainObject1 -> domainObject1.getIdentityKey().equals(domainObject.getIdentityKey()))
                            .findFirst()
                            .orElseThrow(() -> new DataMapperException("Previous state of the updated domainObject not found"));
                    MapperRegistry.getExternal(domainObject.getClass()).updateReferences(prevDomainObj, domainObject);
                });
                iterate(deletedReposIterator, deletedObjectsIterator, (repo, domainObject) -> {
                    repo.invalidate(domainObject.getIdentityKey());
                    MapperRegistry.getExternal(domainObject.getClass()).removeReferences(domainObject);
                });
            }

//            SQLUtils.<Void>callbackToPromise(ar -> connection.commit(ar))
//                    .thenAccept(v -> closeConnection())
//                    .thenAccept(v -> logger.info("{} - Changes have been committed", this.hashCode()))
//                    .join();

//        } catch (Exception e) {
//            logger.info(UNSUCCESSFUL_COMMIT_MESSAGE, this.hashCode(), e.getMessage());
//            rollback();
//        } finally {
//            newObjects.clear();
//            clonedObjects.clear();
//            dirtyObjects.clear();
//            removedObjects.clear();
//        }
    }

    private <V> void iterate(Iterator<DataRepository<? extends DomainObject<?>, ?>> repoIterator, Iterator<DomainObject> domainObjectIterator,
                             BiConsumer<DataRepository<DomainObject<V>, V>, DomainObject<V>> biConsumer) {
        if (repoIterator.hasNext()) {
            DataRepository<DomainObject<V>, V> repo = (DataRepository<DomainObject<V>, V>) repoIterator.next();
            DomainObject<V> domainObject = domainObjectIterator.next();
            biConsumer.accept(repo, domainObject);
        }
    }

    /**
     * It will iterate over {@code list} and call {@code biFunction} passing the mapper and the domainObject
     *
     * @param biFunction the biFunction to be called for each iteration
     * @param list the list to iterate
     * @param predicate the predicate to filter the elements to iterate
     * @return a Pair whose key contains an ordered List of the object's mappers inside {@code list}, meaning, for example,
     * for object located at index 4 in {@code list}, its mapper is located at the same index in this List.
     *
     * The value of the Pair is a List containing the completableFutures of the calls of the mapper
     */
    private<V> Pair<List<DataRepository<? extends DomainObject<?>, ?>>, List<CompletableFuture<Void>>> executeFilteredBiFunctionInList(
            BiFunction<Mapper<DomainObject<V>, V>, DomainObject<V>, CompletableFuture<Void>> biFunction,
            Queue<DomainObject> list,
            Predicate<DomainObject> predicate
    ) {
        List<DataRepository<? extends DomainObject<?>, ?>> mappers = new ArrayList<>();
        List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
        list
                .stream()
                .filter(predicate)
                .forEach(domainObject -> {
                    DataRepository repository = getRepository(domainObject.getClass());
                    completableFutures.add(UnitOfWork.executeActionWithinUnit(this, () -> biFunction.apply(repository.getMapper(), domainObject)));
                    mappers.add(repository);
                });

        return new Pair<>(mappers, completableFutures);
    }

    /**
     * Removes the objects from the newObjects from the IdentityMap
     * Puts the objects in removedObjects into the IdentityMap
     * The objects in dirtyObjects need to go back as before
     */
    public CompletableFuture<Void> rollback() {
        try {
            System.out.println("connection in rollbak " + connection);
            if(connection != null) {
                return connection.thenCompose(con -> SQLUtils.callbackToPromise(con::rollback))
                        .thenCompose(v -> closeConnection());
            }

            newObjects.forEach(domainObject -> getRepository(domainObject.getClass()).invalidate(domainObject.getIdentityKey()));

            for (DomainObject obj : dirtyObjects) {
                clonedObjects
                        .stream()
                        .filter(domainObject -> domainObject.getIdentityKey().equals(obj.getIdentityKey()))
                        .findFirst()
                        .ifPresent(
                                clone -> getRepository(obj.getClass()).validate(clone.getIdentityKey(), clone)
                        );
            }

            removedObjects
                    .stream()
                    .filter(obj -> !dirtyObjects.contains(obj))
                    .forEach(obj -> getRepository(obj.getClass()).validate(obj.getIdentityKey(), obj));

            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            throw new DataMapperException(e);
        }
    }
}
