package org.github.isel.rapper;

import org.github.isel.rapper.utils.ConnectionManager;
import org.github.isel.rapper.utils.MapperRegistry;
import org.github.isel.rapper.utils.UnitOfWork;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

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
        Mapper<T, K> mapper = MapperRegistry.getMapper(type);
        ConcurrentMap<K, T> identityMap = ((DataMapper<T, K>) mapper).getIdentityMap();
        if(identityMap.containsKey(t.getIdentityKey())){
            identityMap.get(t.getIdentityKey()).markToBeDirty();
        }
        t.markDirty();
        return UnitOfWork.getCurrent().commit();
    }

    @Override
    public CompletableFuture<Boolean> updateAll(Iterable<T> t) {
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
    public CompletableFuture<Boolean> deleteById(K k) {
        checkUnitOfWork();
        Mapper<T, K> mapper = MapperRegistry.getMapper(type);
        ConcurrentMap<K, T> identityMap = ((DataMapper<T, K>) mapper).getIdentityMap();
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
}
