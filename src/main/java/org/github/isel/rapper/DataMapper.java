package org.github.isel.rapper;

import org.github.isel.rapper.exceptions.ConcurrencyException;
import org.github.isel.rapper.exceptions.DataMapperException;
import org.github.isel.rapper.utils.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class DataMapper<T extends DomainObject<K>, K> implements Mapper<T, K> {

    private final ConcurrentMap<K,T> identityMap = new ConcurrentHashMap<>();
    private final Class<T> type;
    private final Class<? super T> subClass;
    private final Class<K> keyType;
    private final MapperSettings mapperSettings;

    public MapperSettings getMapperSettings() {
        return mapperSettings;
    }

    public DataMapper(Class<T> type, Class<K> keyType){
        this.type = type;
        this.keyType = keyType;
        this.subClass = type.getSuperclass();
        this.mapperSettings = new MapperSettings(type);
    }

    private Stream<T> stream(Statement statement, ResultSet rs, Function<ResultSet, T> func) throws DataMapperException{
        return StreamSupport.stream(new Spliterators.AbstractSpliterator<T>(
                Long.MAX_VALUE, Spliterator.ORDERED) {
            @Override
            public boolean tryAdvance(Consumer<? super T> action) {
                try {
                    if(!rs.next())return false;
                    action.accept(func.apply(rs));
                    return true;
                } catch (SQLException e) {
                    throw new DataMapperException(e);
                }
            }
        }, false).onClose(() -> {
            try {
                rs.close();
                statement.close();
            } catch (SQLException e) {
                throw new DataMapperException(e.getMessage(), e);
            }
        }).peek(this::putOrReplace);
    }

    private void putOrReplace(T item){
        K key = item.getIdentityKey();
        identityMap.compute(key, (k,v)-> item);
    }

    private T mapper(ResultSet rs){
        try {
            Constructor<T> c = type.getConstructor(mapperSettings.getFields().stream().map(Field::getType).toArray(Class[]::new));

            return c.newInstance(mapperSettings.getFields().stream().map(((SqlFunction<Field,Object>)f->
                    rs.getObject(f.getName())).wrap()).toArray());

        } catch (NoSuchMethodException e) {//TODO
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void setIds(PreparedStatement stmt, K id, int offset){
        SqlFunction<Field, Object> func;
        if(mapperSettings.getId().size()>1){
            func = f -> {
                    f.setAccessible(true);
                    return f.get(id);
            };
        } else {
            func = f -> id;
        }
        SqlConsumer<Map.Entry<Integer, Field>> consumer = entry-> stmt.setObject(entry.getKey() + offset, func.apply(entry.getValue()));
        CollectionUtils.zipWithIndex(mapperSettings.getId().stream()).forEach(consumer.wrap());
    }

    private void setColumns(PreparedStatement stmt, T obj, int offset) {
        SqlFunction<Field, Object> func = f -> {
                f.setAccessible(true);
                return f.get(obj);
        };
        SqlConsumer<Map.Entry<Integer, Field>> consumer = entry -> stmt.setObject(entry.getKey() +offset, func.apply(entry.getValue()));
        CollectionUtils.zipWithIndex(mapperSettings.getColumns().stream()).forEach(consumer.wrap());
    }

    private boolean tryReplace(T obj, long timeout){
        long target = System.currentTimeMillis() +  timeout;
        long remaining = target - System.currentTimeMillis();

        while(remaining >= 0){
            T observedObj = identityMap.get(obj.getIdentityKey());
            if(observedObj.getVersion() < obj.getVersion()) {
                if(identityMap.replace(obj.getIdentityKey(), observedObj, obj))
                    return true;
            }
            remaining = target - System.currentTimeMillis();
            Thread.yield();
        }
        return false;
    }

    @Override
    public CompletableFuture<Optional<T>> getById(K id) {
        T obj = identityMap.get(id);
        if(obj != null)
            return CompletableFuture.completedFuture(Optional.of(obj));
        SqlFunction<PreparedStatement, Stream<T>> func = stmt -> stream(stmt, stmt.getResultSet(), this::mapper);
        return SQLUtils.execute(mapperSettings.getSelectByIdQuery(), stmt -> setIds(stmt, id, 0))
                .thenApply(func.wrap())
                .thenApply(Stream::findFirst);
    }

    @Override
    public CompletableFuture<List<T>> getAll() {
        SqlFunction<PreparedStatement, Stream<T>> func = stmt -> stream(stmt, stmt.getResultSet(), this::mapper);
        return SQLUtils.execute(mapperSettings.getSelectQuery(), s ->{})
                .thenApply(func.wrap())
                .thenApply(s->s.collect(Collectors.toList()));
    }

    @Override
    public void insert(T obj) {
        SqlFunction<PreparedStatement, T> func = s -> setVersion(s, obj);
        func = func.compose(s -> {
            if(mapperSettings.isIdentity()){
                mapperSettings.getId().stream().findFirst().ifPresent(((SqlConsumer<Field>)f -> {
                    f.setAccessible(true);
                    f.set(obj, SQLUtils.getGeneratedKey(s));
                }).wrap());
            }
            return s;
        });
        SQLUtils.execute(mapperSettings.getInsertQuery(), s -> {
            if(!mapperSettings.isIdentity())
                setIds(s, obj.getIdentityKey(), 0);
            setColumns(s, obj, mapperSettings.getId().size()-1);
        }).thenApply(func.wrap()).thenAccept(o -> identityMap.put(o.getIdentityKey(), obj));
    }

    private T setVersion(PreparedStatement s, T obj) throws SQLException, NoSuchFieldException, IllegalAccessException {
        if(s.getUpdateCount()==0) throw new ConcurrencyException("Concurrency problem found");
        Field v = type.getField("version");
        v.setAccessible(true);
        v.set(obj, SQLUtils.getVersion(s));
        return obj;
    }

    @Override
    public void update(T obj) {
        SqlFunction<PreparedStatement, T> func = s -> setVersion(s, obj);
        SQLUtils.execute(mapperSettings.getUpdateQuery(), stmt -> {
            setColumns(stmt, obj, 0);
            setIds(stmt, obj.getIdentityKey(), mapperSettings.getColumns().size()-1);
        })
                .thenApply(func.wrap())
                .thenAccept(o -> {
                    if(!tryReplace(o, 5000)) throw new ConcurrencyException("Concurrency problem found, could not update IdentityMap");
                });
    }

    @Override
    public void delete(T obj) {
        SQLUtils.execute(mapperSettings.getDeleteQuery(), stmt -> setIds(stmt, obj.getIdentityKey(), 0))
                .thenAccept(v -> identityMap.remove(obj.getIdentityKey()));
    }

    public ConcurrentMap<K, T> getIdentityMap() {
        return identityMap;
    }

    public String getSelectQuery() {
        return mapperSettings.getSelectQuery();
    }

    public String getInsertQuery() {
        return mapperSettings.getInsertQuery();
    }

    public String getUpdateQuery() {
        return mapperSettings.getUpdateQuery();
    }

    public String getDeleteQuery() {
        return mapperSettings.getDeleteQuery();
    }
}
