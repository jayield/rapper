package com.github.jayield.rapper.utils;


import com.github.jayield.rapper.DataRepository;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.Mapper;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.exceptions.UnitOfWorkException;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.github.jayield.rapper.utils.MapperRegistry.getExternal;
import static com.github.jayield.rapper.utils.MapperRegistry.getRepository;

public class UnitOfWork {
    /**
     * Private for each transaction
     */
    private Connection connection = null;
    private final Logger logger = LoggerFactory.getLogger(UnitOfWork.class);
    private final Supplier<Connection> connectionSupplier;

    //Multiple Threads may be accessing the Queue, so it must be a ConcurrentLinkedQueue
    private final Queue<DomainObject> newObjects = new ConcurrentLinkedQueue<>();
    private final Queue<DomainObject> clonedObjects = new ConcurrentLinkedQueue<>();
    private final Queue<DomainObject> dirtyObjects = new ConcurrentLinkedQueue<>();
    private final Queue<DomainObject> removedObjects = new ConcurrentLinkedQueue<>();

    private UnitOfWork(Supplier<Connection> connectionSupplier){
        this.connectionSupplier = connectionSupplier;
    }

    public Connection getConnection() {
        if(connection == null)
            connection = connectionSupplier.get();
        return connection;
    }

    public void closeConnection(){
        try {
            connection.close();
        } catch (SQLException e) {
            logger.info("Error closing connection\nError Message: " + e.getMessage());
        }
        connection = null;
    }

    /**
     * Adds the obj to the newObjects List and to the IdentityMap
     * @param obj
     */
    public void registerNew(DomainObject obj) {
        assert obj.getIdentityKey() != null;
        assert !dirtyObjects.contains(obj);
        assert !removedObjects.contains(obj);
        assert !newObjects.contains(obj);
        newObjects.add(obj);
    }

    /**
     * It will be created a clone of obj, in case a rollback is done, we have a way to go back as it was before
     * @param obj DomainObject to be cloned
     */
    public void registerClone(DomainObject obj) {
        assert obj.getIdentityKey()!= null;
        assert !removedObjects.contains(obj);
        if(!clonedObjects.contains(obj) && !newObjects.contains(obj))
            clonedObjects.add(obj);
    }

    /**
     * Tags the object to be updated on the DB
     * @param obj
     */
    public void registerDirty(DomainObject obj){
        assert obj.getIdentityKey()!= null;
        assert !removedObjects.contains(obj);
        if(!dirtyObjects.contains(obj) && !newObjects.contains(obj))
            dirtyObjects.add(obj);
    }

    /**
     * Removes the obj from newObjects and/or dirtyObjects and from the IdentityMap
     * @param obj
     */
    public void registerRemoved(DomainObject obj){
        assert obj.getIdentityKey()!= null;
        if(newObjects.remove(obj)) return;
        dirtyObjects.remove(obj);
        if(!removedObjects.contains(obj))
            removedObjects.add(obj);
    }

    private static ThreadLocal<UnitOfWork> current = new ThreadLocal<>();

    /**
     * Each Thread will have its own UnitOfWork
     */
    public static void newCurrent(Supplier<Connection> supplier) {
        setCurrent(new UnitOfWork(supplier));
    }

    public static void setCurrent(UnitOfWork uow) {
        current.set(uow);
    }

    public static void removeCurrent(){
        current.remove();
    }

    public static UnitOfWork getCurrent() {
        UnitOfWork unitOfWork = current.get();
        if(unitOfWork == null)
            throw new UnitOfWorkException("The Unit of Work you're trying to access is currently NULL. You must create or set it first.");
        return unitOfWork;
    }

    public CompletableFuture<Boolean> commit() {
        Pair<List<DataRepository<? extends DomainObject<?>, ?>>,
                List<CompletableFuture<Boolean>>> insertPair = executeFilteredBiFunctionInList(Mapper::create, newObjects, domainObject -> true);

        Pair<List<DataRepository<? extends DomainObject<?>, ?>>,
                List<CompletableFuture<Boolean>>> updatePair = executeFilteredBiFunctionInList(Mapper::update, dirtyObjects, domainObject -> !removedObjects.contains(domainObject));

        Pair<List<DataRepository<? extends DomainObject<?>, ?>>,
                List<CompletableFuture<Boolean>>> deletePair = executeFilteredBiFunctionInList(Mapper::delete, removedObjects, domainObject -> true);

        List<CompletableFuture<Boolean>> completableFutures = insertPair.getValue();
        completableFutures.addAll(updatePair.getValue());
        completableFutures.addAll(deletePair.getValue());

        return completableFutures
                .stream()
                .reduce(CompletableFuture.completedFuture(true), (a, b) -> a.thenCombine(b, (a2, b2) -> a2 && b2))
                .thenApply(success -> updateIdentityMap(success, insertPair.getKey(), updatePair.getKey(), deletePair.getKey()));
    }

    private boolean updateIdentityMap(boolean success, List<DataRepository<? extends DomainObject<?>, ?>> insertRepos, List<DataRepository<? extends DomainObject<?>, ?>> updateRepos,
                                      List<DataRepository<? extends DomainObject<?>, ?>> deleteRepos) {
        try {
            if(!success) {
                rollback();
                return false;
            }

            //The different iterators will have the same size (eg. insertedReposIterator.size() == insertedObjectsIterator.size()
            Iterator<DataRepository<? extends DomainObject<?>, ?>> insertedReposIterator = insertRepos.iterator(),
                    updatedReposIterator = updateRepos.iterator(),
                    deletedReposIterator = deleteRepos.iterator();
            Iterator<DomainObject> insertedObjectsIterator = newObjects.iterator(),
                    updatedObjectsIterator = dirtyObjects.iterator(),
                    deletedObjectsIterator = removedObjects.iterator();

            //Will iterate through insert, update and delete iterators at the same time
            while (insertedReposIterator.hasNext() || updatedReposIterator.hasNext() || deletedReposIterator.hasNext()) {
                iterate(insertedReposIterator, insertedObjectsIterator, (repo, domainObject) -> {
                    repo.validate(domainObject.getIdentityKey(), domainObject);
                    getExternal(domainObject.getClass()).insertReferences(domainObject);
                });
                iterate(updatedReposIterator, updatedObjectsIterator, (repo, domainObject) -> {
                    repo.validate(domainObject.getIdentityKey(), domainObject);
                    DomainObject prevDomainObj = clonedObjects
                            .stream()
                            .filter(domainObject1 -> domainObject1.getIdentityKey().equals(domainObject.getIdentityKey()))
                            .findFirst()
                            .orElseThrow(() -> new DataMapperException("Previous state of the updated domainObject not found"));
                    getExternal(domainObject.getClass()).updateReferences(prevDomainObj, domainObject);
                });
                iterate(deletedReposIterator, deletedObjectsIterator, (repo, domainObject) -> {
                    repo.invalidate(domainObject.getIdentityKey());
                    getExternal(domainObject.getClass()).removeReferences(domainObject);
                });
            }

            connection.commit();
            return true;
        } catch (SQLException e) {
            logger.info("Commit wasn't successful, here's the error message:\n" + e.getMessage());
            rollback();
            return false;
        } finally {
            closeConnection();
            newObjects.clear();
            clonedObjects.clear();
            dirtyObjects.clear();
            removedObjects.clear();
        }
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
    private<V> Pair<List<DataRepository<? extends DomainObject<?>, ?>>, List<CompletableFuture<Boolean>>> executeFilteredBiFunctionInList(
            BiFunction<Mapper<DomainObject<V>, V>, DomainObject<V>, CompletableFuture<Boolean>> biFunction,
            Queue<DomainObject> list,
            Predicate<DomainObject> predicate
    ) {
        List<DataRepository<? extends DomainObject<?>, ?>> mappers = new ArrayList<>();
        List<CompletableFuture<Boolean>> completableFutures = new ArrayList<>();
        list
                .stream()
                .filter(predicate)
                .forEach(domainObject -> {
                    DataRepository repository = getRepository(domainObject.getClass());
                    completableFutures.add(biFunction.apply(repository.getMapper(), domainObject));
                    mappers.add(repository);
                });

        return new Pair<>(mappers, completableFutures);
    }

    /**
     * Removes the objects from the newObjects from the IdentityMap
     * Puts the objects in removedObjects into the IdentityMap
     * The objects in dirtyObjects need to go back as before
     */
    public void rollback() {
        try {
            connection.rollback();

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
        } catch (SQLException e) {
            throw new DataMapperException(e);
        }
    }
}
