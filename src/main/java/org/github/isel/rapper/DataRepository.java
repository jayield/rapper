package org.github.isel.rapper;

import org.github.isel.rapper.utils.ConnectionManager;
import org.github.isel.rapper.utils.MapperRegistry;
import org.github.isel.rapper.utils.UnitOfWork;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static org.github.isel.rapper.utils.ConnectionManager.*;

public class DataRepository<T extends DomainObject<K>, K> implements Mapper<T, K> {

    private final ConcurrentMap<K, T> identityMap = new ConcurrentHashMap<>();
    private final Class<T> type;
    private final DataMapper<T, K> mapper;

    public DataRepository(Class<T> type){
        this.type = type;
        mapper = new DataMapper<>(type);
    }

    public DataMapper<T, K> getMapper() {
        return mapper;
    }

    private void checkUnitOfWork(){
        if(UnitOfWork.getCurrent() == null) {
            ConnectionManager connectionManager = ConnectionManager.getConnectionManager(DBsPath.DEFAULTDB);
            UnitOfWork.newCurrent(connectionManager::getConnection);
        }
    }

    @Override
    public CompletableFuture<Optional<T>> findById(K k) {
        checkUnitOfWork();
        if(identityMap.containsKey(k)){
            return CompletableFuture.completedFuture(Optional.of(identityMap.get(k)));
        }
        return mapper.findById(k).thenApply(t -> { t.ifPresent(this::putOrReplace); return t; });
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
            if(identityMap.containsKey(t1.getIdentityKey())){
                identityMap.get(t1.getIdentityKey()).markToBeDirty();
            }
            t1.markDirty();
        });
        return UnitOfWork.getCurrent().commit();
    }

    @Override
    public CompletableFuture<Boolean> deleteById(K k) {
        checkUnitOfWork();
        if(identityMap.containsKey(k)){
            identityMap.get(k).markRemoved();
            return UnitOfWork.getCurrent().commit();
        }
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
        List<CompletableFuture<Boolean>> list = new ArrayList<>();
        keys.forEach(k -> list.add(deleteById(k)));
        return list
                .stream()
                .reduce(CompletableFuture.completedFuture(true), (a, b) -> a.thenCombine(b, (a2, b2) -> a2 && b2));
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
