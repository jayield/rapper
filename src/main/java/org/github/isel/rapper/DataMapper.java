package org.github.isel.rapper;

import javafx.util.Pair;
import org.github.isel.rapper.exceptions.ConcurrencyException;
import org.github.isel.rapper.exceptions.DataMapperException;
import org.github.isel.rapper.utils.*;
import org.github.isel.rapper.utils.SqlField.SqlFieldId;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class DataMapper<T extends DomainObject<K>, K> implements Mapper<T, K> {

    private final ConcurrentMap<K,T> identityMap = new ConcurrentHashMap<>();
    private final Class<T> type;
    private final Class<? super T> subClass;
    private final MapperSettings mapperSettings;
    private final Constructor<T> constructor;

    public MapperSettings getMapperSettings() {
        return mapperSettings;
    }

    public DataMapper(Class<T> type){
        this.type = type;
        this.subClass = type.getSuperclass();
        this.mapperSettings = new MapperSettings(type);
        try {
            this.constructor = type.getConstructor(mapperSettings.getAllFields().stream().map(f->f.field.getType()).toArray(Class[]::new));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

    }

    private Stream<T> stream(Statement statement, ResultSet rs) throws DataMapperException{
        return StreamSupport.stream(new Spliterators.AbstractSpliterator<T>(
                Long.MAX_VALUE, Spliterator.ORDERED) {
            @Override
            public boolean tryAdvance(Consumer<? super T> action) {
                try {
                    if(!rs.next())return false;
                    action.accept(mapper(rs));
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
            SqlFunction<SqlField, Object> rsGetter = f -> rs.getObject(f.name);
            Object[] args = mapperSettings
                    .getAllFields()
                    .stream()
                    .map(rsGetter.wrap())
                    .toArray();
            return constructor.newInstance(args);
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    protected<R> CompletableFuture<List<T>> findWhere(Pair<String, R>... values){
        String query = Arrays.stream(values)
                .map(p -> p.getKey() + " = ? ")
                .collect(Collectors.joining(" AND ", mapperSettings.getSelectQuery() + " WHERE ", ""));
        SqlConsumer<PreparedStatement> consumer = s -> {
            for (int i = 0; i < values.length; i++) {
                s.setObject(i+1, values[i].getValue());
            }
        };
        return SQLUtils.execute(query, consumer.wrap())
                .thenApply(((SqlFunction<PreparedStatement, Stream<T>>)s -> stream(s, s.getResultSet())).wrap())
                .thenApply(s -> s.collect(Collectors.toList()));
    }

    private void setIds(PreparedStatement stmt, K id, int offset){
        SqlFunction<Field, Object> func;
        if(mapperSettings.getIds().size()>1){
            func = f -> {
                    f.setAccessible(true);
                    return f.get(id);
            };
        } else {
            func = f -> id;
        }
        SqlConsumer<Map.Entry<Integer, SqlFieldId>> consumer = entry-> {
            stmt.setObject(entry.getKey() + offset +1 , func.apply(entry.getValue().field));
        };
        CollectionUtils.zipWithIndex(mapperSettings.getIds().stream()).forEach(consumer.wrap());
    }

    private void setColumns(PreparedStatement stmt, T obj, int offset) {
        SqlFunction<Field, Object> func = f -> {
                f.setAccessible(true);
                return f.get(obj);
        };
        SqlConsumer<Map.Entry<Integer, SqlField>> consumer = entry -> {
            stmt.setObject(entry.getKey() +offset+1, func.apply(entry.getValue().field));
        };
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
        System.out.println(obj);
        if(obj != null)
            return CompletableFuture.completedFuture(Optional.of(obj));
        SqlFunction<PreparedStatement, Stream<T>> func = stmt -> stream(stmt, stmt.getResultSet());
        return SQLUtils.execute(mapperSettings.getSelectByIdQuery(), stmt -> setIds(stmt, id, 0))
                .thenApply(func.wrap())
                .thenApply(Stream::findFirst);
    }

    @Override
    public CompletableFuture<List<T>> getAll() {
        SqlFunction<PreparedStatement, Stream<T>> func = stmt -> stream(stmt, stmt.getResultSet());
        return SQLUtils.execute(mapperSettings.getSelectQuery(), s ->{})
                .thenApply(func.wrap())
                .thenApply(s->s.collect(Collectors.toList()));
    }

    @Override
    public void insert(T obj) {
        SqlFunction<PreparedStatement, T> func = s -> setVersion(s, obj);
        func = func.compose(s -> {
            System.out.println(s.getUpdateCount());
            if((mapperSettings.getIds().stream().noneMatch(f->f.identity))){
                mapperSettings.getIds().stream().map(f->f.field).findFirst().ifPresent(((SqlConsumer<Field>)f -> {
                    f.setAccessible(true);
                    f.set(obj, SQLUtils.getGeneratedKey(s));
                }).wrap());
            }
            return s;
        });

        //This is only done once because it will be recursive (The parentClass will check if its parentClass is a DomainObject and therefore call its insert)
        Class<? super T> aClass = type.getSuperclass();
        if(aClass != Object.class && DomainObject.class.isAssignableFrom(aClass)) {
            DataMapper classMapper = MapperRegistry.getMapper((Class<DomainObject>) aClass);
            classMapper.insert(obj);
        }
        //If the id is autogenerated, it will be set on the obj by the insert of the parent

        SQLUtils.execute(mapperSettings.getInsertQuery(), s -> {
            if(mapperSettings.getIds().stream().noneMatch(f->f.identity))
                setIds(s, obj.getIdentityKey(), 0);
            setColumns(s, obj, mapperSettings.getIds().size());
        })
                .thenApply(func.wrap())
                .thenAccept(o -> identityMap.put(o.getIdentityKey(), obj))
                .join();
    }

    private List<Class<DomainObject>> getClassParents() {
        List<Class<DomainObject>> parents = new ArrayList<>();
        Class<? super T> aclass = type.getSuperclass();
        for( ; aclass != Object.class && DomainObject.class.isAssignableFrom(aclass); aclass = aclass.getSuperclass()){
            parents.add((Class<DomainObject>) aclass);
        }
        return parents;
    }

    private T setVersion(PreparedStatement s, T obj) throws SQLException, IllegalAccessException {
        if(s.getUpdateCount()==0) throw new ConcurrencyException("Concurrency problem found");
        Field v = null;
        try {
            v = type.getField("version");
            v.setAccessible(true);
            v.set(obj, SQLUtils.getVersion(s));
        } catch (NoSuchFieldException ignored) { }

        return obj;
    }

    @Override
    public void update(T obj) {
        SqlFunction<PreparedStatement, T> func = s -> setVersion(s, obj);
        SQLUtils.execute(mapperSettings.getUpdateQuery(), stmt -> {
            setColumns(stmt, obj, 0);
            setIds(stmt, obj.getIdentityKey(), mapperSettings.getColumns().size());
        })
                .thenApply(func.wrap())
                .thenApply(t -> {
                    Class<? super T> aClass = type.getSuperclass();
                    if(aClass != Object.class && DomainObject.class.isAssignableFrom(aClass)) {
                        DataMapper classMapper = MapperRegistry.getMapper((Class<DomainObject>) aClass);
                        classMapper.update(obj);
                    }
                    return t;
                })
                .thenAccept(o -> {
                    if(!tryReplace(o, 5000)) throw new ConcurrencyException("Concurrency problem found, could not update IdentityMap");
                }).join();
    }

    @Override
    public void delete(T obj) {
        SQLUtils.execute(mapperSettings.getDeleteQuery(), stmt -> setIds(stmt, obj.getIdentityKey(), 0))
                .thenAccept(s -> {
                    Class<? super T> aClass = type.getSuperclass();
                    if(aClass != Object.class && DomainObject.class.isAssignableFrom(aClass)) {
                        DataMapper classMapper = MapperRegistry.getMapper((Class<DomainObject>) aClass);
                        classMapper.delete(obj);
                    }
                })
                .thenAccept(v -> identityMap.remove(obj.getIdentityKey())).join();
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
