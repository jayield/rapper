package org.github.isel.rapper;

import org.github.isel.rapper.utils.ConnectionManager;
import org.github.isel.rapper.utils.MapperRegistry;
import org.github.isel.rapper.utils.UnitOfWork;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import static org.github.isel.rapper.utils.ConnectionManager.*;

public class DataRepository<T extends DomainObject<K>, K> implements Repository<T, K> {

    private final Class<T> type;
    private ConnectionManager connectionManager;

    public DataRepository(Class<T> type){
        this.type = type;
        connectionManager = getConnectionManager(DBsPath.DEFAULTDB);
    }

    private void checkUnitOfWork(){
        if(UnitOfWork.getCurrent() == null)
            UnitOfWork.newCurrent(() -> connectionManager.getConnection());
    }

    @Override
    public CompletableFuture<Optional<T>> findById(K k) {
        checkUnitOfWork();
        Mapper<T, K> mapper = MapperRegistry.getMapper(type);
        ConcurrentMap<K, T> identityMap = ((DataMapper<T, K>) mapper).getIdentityMap();
        if(identityMap.containsKey(k)){
            return CompletableFuture.completedFuture(Optional.of(identityMap.get(k)));
        }
        return mapper.getById(k);
    }

    @Override
    public CompletableFuture<List<T>> findAll() {
        checkUnitOfWork();
        Mapper<T, K> mapper = MapperRegistry.getMapper(type);
        CompletableFuture<List<T>> completableFuture = mapper.getAll();
        completableFuture.thenAccept(list -> list.forEach(DomainObject::markClean));
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> create(T t) {
        checkUnitOfWork();
        t.markNew();
        return UnitOfWork.getCurrent().commit();
    }

    @Override
    public CompletableFuture<Void> createAll(Iterable<T> t) {
        checkUnitOfWork();
        t.forEach(DomainObject::markNew);
        return UnitOfWork.getCurrent().commit();
    }

    @Override
    public CompletableFuture<Void> update(T t) {
        checkUnitOfWork();
        Mapper<T, K> mapper = MapperRegistry.getMapper(type);
        ConcurrentMap<K, T> identityMap = ((DataMapper<T, K>) mapper).getIdentityMap();
        if(identityMap.containsKey(t.getIdentityKey())){
            identityMap.get(t.getIdentityKey()).markToBeDirty();
        }
        t.markDirty();
        return UnitOfWork.getCurrent().commit();
    }

    @Override
    public CompletableFuture<Void> updateAll(Iterable<T> t) {
        checkUnitOfWork();
        Mapper<T, K> mapper = MapperRegistry.getMapper(type);
        ConcurrentMap<K, T> identityMap = ((DataMapper<T, K>) mapper).getIdentityMap();
        t.forEach(t1 -> {
            if(identityMap.containsKey(t1.getIdentityKey())){
                identityMap.get(t1.getIdentityKey()).markToBeDirty();
            }
            t1.markDirty();
        });
        return UnitOfWork.getCurrent().commit();
    }

    @Override
    public CompletableFuture<Void> deleteById(K k) {
        checkUnitOfWork();
        Mapper<T, K> mapper = MapperRegistry.getMapper(type);
        ConcurrentMap<K, T> identityMap = ((DataMapper<T, K>) mapper).getIdentityMap();
        if(identityMap.containsKey(k)){
            identityMap.get(k).markRemoved();
            return UnitOfWork.getCurrent().commit();
        }
        else {
            Consumer<T> consumer = t1 -> {
                t1.markRemoved();
                UnitOfWork.getCurrent().commit().join();
            };

            return findById(k)
                    .thenAccept(t -> t.ifPresent(consumer));
        }
    }

    @Override
    public CompletableFuture<Void> delete(T t) {
        checkUnitOfWork();
        t.markRemoved();
        return UnitOfWork.getCurrent().commit();
    }

    @Override
    public CompletableFuture<Void> deleteAll(Iterable<K> keys) {
        checkUnitOfWork();
        List<CompletableFuture<Void>> list = new ArrayList<>();
        keys.forEach(k -> list.add(deleteById(k)));
        return CompletableFuture.allOf((CompletableFuture<Void>[]) list.toArray());
    }
}
