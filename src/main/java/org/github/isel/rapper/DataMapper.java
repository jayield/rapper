package org.github.isel.rapper;

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

            return c.newInstance(mapperSettings.getFields().stream().map(f-> {
                try {
                    return rs.getObject(f.getName());
                } catch (SQLException e) {
                    throw new DataMapperException(e);
                }
            }).toArray());

        } catch (NoSuchMethodException e) {
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

    private <U> CompletableFuture<U> execute(String sqlQuery, SqlFunction<PreparedStatement, U> handleStatement){
        Connection con = UnitOfWork.getCurrent().getConnection();
        try{
            PreparedStatement preparedStatement = con.prepareStatement(sqlQuery);
            return CompletableFuture.supplyAsync(()-> handleStatement.wrap().apply(preparedStatement));
        } catch (SQLException e) {
            throw new DataMapperException(e);
        }
    }

    public void getById2(K id){
        Function<Field, Object> func;
        if(mapperSettings.getId().size()>1){
            func = f -> {
                try {
                    f.setAccessible(true);
                    return f.get(id);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            };
        } else {
            func = f -> id;
        }
        SqlConsumer<Map.Entry<Integer, Field>> consumer = entry-> System.out.println(entry.getKey() + ":" + func.apply(entry.getValue()));
        CollectionUtils.zipWithIndex(mapperSettings.getId().stream()).forEach(consumer.wrap());
    }

    private void setIds(PreparedStatement stmt, K id){
        Function<Field, Object> func;
        if(mapperSettings.getId().size()>1){
            func = f -> {
                try {
                    f.setAccessible(true);
                    return f.get(id);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            };
        } else {
            func = f -> id;
        }
        SqlConsumer<Map.Entry<Integer, Field>> consumer = entry-> stmt.setObject(entry.getKey(), func.apply(entry.getValue()));
        CollectionUtils.zipWithIndex(mapperSettings.getId().stream()).forEach(consumer.wrap());
    }

    @Override
    public CompletableFuture<Optional<T>> getById(K id) {
        return execute(mapperSettings.getSelectByIdQuery(), stmt -> {
            setIds(stmt, id);
            return stream(stmt, stmt.executeQuery(), this::mapper);
        }).thenApply(Stream::findFirst);
    }

//    private PreparedStatement m(PreparedStatement stmt, ){
//        if(mapperSettings.getId().size() > 1){
//            mapperSettings.getId()
//        }
//    }

    @Override
    public CompletableFuture<List<T>> getAll() {
        return execute(mapperSettings.getSelectQuery(), stmt -> stream(stmt, stmt.executeQuery(), this::mapper))
                .thenApply(s->s.collect(Collectors.toList()));
    }

    @Override
    public void insert(T obj) {

    }

    @Override
    public void update(T obj) {
        execute(mapperSettings.getUpdateQuery(), stmt -> {
            setColumns(stmt, obj);
            setIds(stmt, obj.getIdentityKey());
            return stmt.executeUpdate()
        })
    }

    @Override
    public void delete(T obj) {
        execute(mapperSettings.getDeleteQuery(), stmt -> {
            setIds(stmt, obj.getIdentityKey());
            return stmt.execute();
        }).thenAccept(v -> identityMap.remove(obj.getIdentityKey()));
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
