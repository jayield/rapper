package org.github.isel.rapper.utils;


import javafx.util.Pair;
import org.github.isel.rapper.DataMapper;
import org.github.isel.rapper.DataRepository;
import org.github.isel.rapper.DomainObject;
import org.github.isel.rapper.exceptions.ConcurrencyException;
import org.github.isel.rapper.exceptions.DataMapperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.github.isel.rapper.utils.MapperRegistry.getRepository;

public class UnitOfWork {
    /**
     * Private for each transaction
     */
    private Connection connection = null;
    private final Logger logger = LoggerFactory.getLogger(UnitOfWork.class);
    private final Supplier<Connection> connectionSupplier;
    private final List<DomainObject> newObjects = new ArrayList<>();
    private final List<DomainObject> clonedObjects = new ArrayList<>();
    private final List<DomainObject> dirtyObjects = new ArrayList<>();
    private final List<DomainObject> removedObjects = new ArrayList<>();

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

    public static UnitOfWork getCurrent() {
        return current.get();
    }

    public CompletableFuture<Boolean> commit() {
        Pair<List<DataRepository<? extends DomainObject<?>, ?>>,
                List<CompletableFuture<Boolean>>> insertPair = executeFilteredBiFunctionInList(DataMapper::create, newObjects, domainObject -> true);

        Pair<List<DataRepository<? extends DomainObject<?>, ?>>,
                List<CompletableFuture<Boolean>>> updatePair = executeFilteredBiFunctionInList(DataMapper::update, dirtyObjects, domainObject -> !removedObjects.contains(domainObject));

        Pair<List<DataRepository<? extends DomainObject<?>, ?>>,
                List<CompletableFuture<Boolean>>> deletePair = executeFilteredBiFunctionInList(DataMapper::delete, removedObjects, domainObject -> true);

        List<CompletableFuture<Boolean>> completableFutures = insertPair.getValue();
        completableFutures.addAll(updatePair.getValue());
        completableFutures.addAll(deletePair.getValue());

        return completableFutures
                .stream()
                .reduce(CompletableFuture.completedFuture(true), (a, b) -> a.thenCombine(b, (a2, b2) -> a2 && b2))
                .thenApply(aBoolean -> { if(aBoolean) return updateIdentityMap(insertPair.getKey(), updatePair.getKey(), deletePair.getKey()); return aBoolean; });
    }

    private Boolean updateIdentityMap(List<DataRepository<? extends DomainObject<?>, ?>> insertMappers, List<DataRepository<? extends DomainObject<?>, ?>> updateMappers,
                                      List<DataRepository<? extends DomainObject<?>, ?>> deleteMappers) {
        try {
            //The different iterators will have the same size (eg. insertMapperIter.size() == insertObjIter.size()
            Iterator<DataRepository<? extends DomainObject<?>, ?>> insertMapperIter = insertMappers.iterator(),
                    updateMapperIter = updateMappers.iterator(),
                    deleteMapperIter = deleteMappers.iterator();
            Iterator<DomainObject> insertObjIter = newObjects.iterator(),
                    updateObjIter = dirtyObjects.iterator(),
                    deleteObjIter = removedObjects.iterator();

            //Will iterate through insert, update and delete iterators at the same time
            while (insertMapperIter.hasNext() || updateMapperIter.hasNext() || deleteMapperIter.hasNext()){
                iterate(insertMapperIter, insertObjIter);
                iterate(updateMapperIter, updateObjIter);
                iterate(deleteMapperIter, deleteObjIter);
            }

            connection.commit();
            return true;
        }
        catch (ConcurrencyException | SQLException e) {
            try {
                logger.info("Commit wasn't successful, here's the error message:\n" +  e.getMessage());
                rollback();
                return false;
            } catch (SQLException sqlException) {
                throw new DataMapperException(sqlException);
            }
        } finally {
            closeConnection();
            newObjects.clear();
            clonedObjects.clear();
            dirtyObjects.clear();
            removedObjects.clear();
        }
    }

    private<V> void iterate(Iterator<DataRepository<? extends DomainObject<?>, ?>> mapperIterator, Iterator<DomainObject> domainObjectIterator) {
        if(mapperIterator.hasNext()){
            DataRepository<DomainObject<V>, V> mapper = (DataRepository<DomainObject<V>, V>) mapperIterator.next();
            DomainObject<V> domainObject = domainObjectIterator.next();
            if(!mapper.tryReplace(domainObject)) {
                throw new ConcurrencyException("Couldn't update IdentityMap");
            }
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
            BiFunction<DataMapper<DomainObject<V>, V>, DomainObject<V>, CompletableFuture<Boolean>> biFunction,
            List<DomainObject> list,
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
    public void rollback() throws SQLException {
        connection.rollback();
        /*for (DomainObject obj : newObjects)
            MapperRegistry.getMapper(obj.getClass()).getIdentityMap().remove(obj.getIdentityKey());*/

        newObjects.forEach(domainObject ->
                getRepository(domainObject.getClass()).invalidate(domainObject.getIdentityKey()));

        for(DomainObject obj : dirtyObjects){
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
    }
}
