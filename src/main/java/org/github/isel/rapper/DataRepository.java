package org.github.isel.rapper;

import javafx.util.Pair;
import org.github.isel.rapper.utils.ConnectionManager;
import org.github.isel.rapper.utils.DBsPath;
import org.github.isel.rapper.utils.SqlSupplier;
import org.github.isel.rapper.utils.UnitOfWork;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

public class DataRepository<T extends DomainObject<K>, K> implements Mapper<T, K> {

    private final ConcurrentMap<K, T> identityMap = new ConcurrentHashMap<>();
    //Used to communicate with the DB
    private final Mapper<T, K> mapper;

    public DataRepository(Mapper<T, K> mapper){
        this.mapper = mapper;
    }

    public Mapper<T, K> getMapper() {
        return mapper;
    }

    private void checkUnitOfWork(){
        if(UnitOfWork.getCurrent() == null) {
            ConnectionManager connectionManager = ConnectionManager.getConnectionManager(DBsPath.DEFAULTDB);
            SqlSupplier<Connection> connectionSupplier = connectionManager::getConnection;
            UnitOfWork.newCurrent(connectionSupplier.wrap());
        }
    }

    @Override
    public <R> CompletableFuture<List<T>> findWhere(Pair<String, R>... values) {
        return mapper.findWhere(values);
    }

    @Override
    public CompletableFuture<Optional<T>> findById(K k) {
        checkUnitOfWork();

        if(identityMap.containsKey(k)){
            return CompletableFuture.completedFuture(Optional.of(identityMap.get(k)));
        }
        return mapper.findById(k).thenApply(t -> { t.ifPresent(this::putOrReplace); return t; });

        /*return CompletableFuture.completedFuture(
                Optional.ofNullable(
                        identityMap.computeIfAbsent(k, k1 -> mapper.findById(k).join().orElse(null))
                )
        );*/

        //return identityMap.computeIfAbsent(k, k1 -> mapper.findById(k).thenApply(t -> t.orElse(null)));
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
        checkUnitOfWork();
        t.markNew();
        return UnitOfWork.getCurrent().commit();
    }

    @Override
    public CompletableFuture<Boolean> createAll(Iterable<T> t) {
        checkUnitOfWork();
        t.forEach(DomainObject::markNew);
        return UnitOfWork.getCurrent().commit();
    }

    @Override
    public CompletableFuture<Boolean> update(T t) {
        checkUnitOfWork();
        if(identityMap.containsKey(t.getIdentityKey())){
            identityMap.get(t.getIdentityKey()).markToBeDirty();
        }
        t.markDirty();
        return UnitOfWork.getCurrent().commit();
    }

    @Override
    public CompletableFuture<Boolean> updateAll(Iterable<T> t) {
        checkUnitOfWork();
        t.forEach(t1 -> {
            identityMap.computeIfPresent(t1.getIdentityKey(), (k, t2) -> {
                t2.markToBeDirty();
                return t2;
            });
            /*if(identityMap.containsKey(t1.getIdentityKey())){
                identityMap.get(t1.getIdentityKey()).markToBeDirty();
            }*/
            t1.markDirty();
        });
        return UnitOfWork.getCurrent().commit();
    }

    @Override
    public CompletableFuture<Boolean> deleteById(K k) {
        checkUnitOfWork();
        CompletableFuture<Boolean>[] future = new CompletableFuture[1];
        identityMap.computeIfPresent(k, (key, t) -> {
            t.markRemoved();
            future[0] = UnitOfWork.getCurrent().commit();
            return t;
        });

        if(future[0] != null) return future[0];
        /*if(identityMap.containsKey(k)){
            identityMap.get(k).markRemoved();
            return UnitOfWork.getCurrent().commit();
        }*/
        else {
            Function<T, Boolean> consumer = t1 -> {
                t1.markRemoved();
                return UnitOfWork.getCurrent().commit().join();
            };

            return findById(k)
                    .thenApply(t -> {
                        if(t.isPresent())
                            return consumer.apply(t.get());
                        else return false;
                    });
        }
    }

    @Override
    public CompletableFuture<Boolean> delete(T t) {
        checkUnitOfWork();
        t.markRemoved();
        return UnitOfWork.getCurrent().commit();
    }

    @Override
    public CompletableFuture<Boolean> deleteAll(Iterable<K> keys) {
        checkUnitOfWork();

        keys.forEach(k -> {
            boolean isPresent = identityMap.computeIfPresent(k, (key, t) -> {
                t.markRemoved();
                return t;
            }) != null;

            if(!isPresent){
                findById(k).join().ifPresent(DomainObject::markRemoved);
            }
        });

        return UnitOfWork.getCurrent().commit();
    }

    private void putOrReplace(T item){
        K key = item.getIdentityKey();
        identityMap.compute(key, (k,v)-> item);
    }

    public void invalidate(K identityKey) {
        identityMap.remove(identityKey);
    }

    public void validate(K identityKey, T t) {
        identityMap.put(identityKey, t);
    }

    public boolean tryReplace(T obj){
        long target = System.currentTimeMillis() + (long) 2000;
        long remaining = target - System.currentTimeMillis();

        while(remaining >= 0){
            T observedObj = identityMap.putIfAbsent(obj.getIdentityKey(), obj);
            if(observedObj == null) return true;
            if(observedObj.getVersion() < obj.getVersion()) {
                if(identityMap.replace(obj.getIdentityKey(), observedObj, obj))
                    return true;
            }
            else return true; //TODO should we log a message saying a newer version is already present in the identityMap?
            remaining = target - System.currentTimeMillis();
            Thread.yield();
        }
        return false;
    }
}
