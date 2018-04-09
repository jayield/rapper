package org.github.isel.rapper;

import javafx.util.Pair;
import jdk.management.resource.internal.inst.StaticInstrumentation;
import org.github.isel.rapper.exceptions.ConcurrencyException;
import org.github.isel.rapper.exceptions.DataMapperException;
import org.github.isel.rapper.utils.*;
import org.github.isel.rapper.utils.SqlField.SqlFieldId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final Logger logger = LoggerFactory.getLogger(DataMapper.class);

    public MapperSettings getMapperSettings() {
        return mapperSettings;
    }

    public DataMapper(Class<T> type){
        this.type = type;
        this.subClass = type.getSuperclass();
        this.mapperSettings = new MapperSettings(type);
        try {
            /*Class[] parameterTypes = mapperSettings
                    .getAllFields()
                    .stream()
                    .map(f -> f.field.getType())
                    .toArray(Class[]::new);*/
            this.constructor = type.getConstructor();
            //this.constructor = type.getConstructor(parameterTypes); //TODO get a better way to get the constructor
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
            T t = constructor.newInstance();

            SqlConsumer<SqlField> fieldSetter = f -> {
                f.field.setAccessible(true);
                f.field.set(t, rs.getObject(f.name));
            };
            mapperSettings
                    .getAllFields()
                    .forEach(fieldSetter.wrap());

            return t;
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
        if(mapperSettings.getIds().size() > 1){
            func = f -> {
                    f.setAccessible(true);
                    return f.get(id);
            };
        } else {
            func = f -> id;
        }
        SqlConsumer<Map.Entry<Integer, SqlFieldId>> consumer = entry -> stmt.setObject(entry.getKey() + offset + 1 , func.apply(entry.getValue().field));
        CollectionUtils.zipWithIndex(mapperSettings.getIds().stream()).forEach(consumer.wrap());
    }

    private void setColumns(PreparedStatement stmt, T obj, int offset) {
        SqlFunction<Field, Object> func = f -> {
                f.setAccessible(true);
                return f.get(obj);
        };
        SqlConsumer<Map.Entry<Integer, SqlField>> consumer = entry -> stmt.setObject(entry.getKey() + offset + 1, func.apply(entry.getValue().field));

        CollectionUtils.zipWithIndex(mapperSettings.getColumns().stream())
                .forEach(consumer.wrap());
    }

    private boolean tryReplace(T obj, long timeout){
        long target = System.currentTimeMillis() +  timeout;
        long remaining = target - System.currentTimeMillis();

        while(remaining >= 0){
            T observedObj = identityMap.putIfAbsent(obj.getIdentityKey(), obj);
            if(observedObj == null) return true;
            if(observedObj.getVersion() < obj.getVersion()) {
                if(identityMap.replace(obj.getIdentityKey(), observedObj, obj))
                    return true;
            }
            else return false;
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

        SqlFunction<PreparedStatement, Stream<T>> func = stmt -> stream(stmt, stmt.getResultSet());
        return SQLUtils.execute(mapperSettings.getSelectByIdQuery(), stmt -> setIds(stmt, id, 0))
                .thenApply(func.wrap())
                .thenApply(Stream::findFirst)
                .thenApply(optionalT -> {
                    optionalT.ifPresent(this::populateExternals);
                    return optionalT;
                });
    }

    @Override
    public CompletableFuture<List<T>> getAll() {
        SqlFunction<PreparedStatement, Stream<T>> func = stmt -> stream(stmt, stmt.getResultSet());
        return SQLUtils.execute(mapperSettings.getSelectQuery(), s ->{})
                .thenApply(func.wrap())
                .thenApply(tStream -> tStream.peek(this::populateExternals))
                .thenApply(tStream1 -> tStream1.collect(Collectors.toList()));
    }

    /**
     * It will go to the DB get the externals for a class (T) and set them on the object received
     * @param t object which externals shall be populated
     */
    private void populateExternals(T t) {
        Consumer<SqlField.SqlFieldExternal> findWhereConsumer = sqlFieldExternal -> {

            SqlConsumer<? super List<T>> sqlConsumer = list -> {
                sqlFieldExternal.field.setAccessible(true);
                sqlFieldExternal.field.set(t, list);
            };

            findWhere(new Pair<>(sqlFieldExternal.columnName, t.getIdentityKey()))
                    .thenAccept(sqlConsumer.wrap());
        };

        List<SqlField.SqlFieldExternal> externals = getMapperSettings().getExternals();
        if(externals != null) externals.forEach(findWhereConsumer);
    }

    /**
     * Gets the mapper of the parent of T.
     * If the parent implements DomainObject (meaning it has a mapper), gets its mapper, else returns an empty Optional.
     * @return Optional of DataMapper or empty Optional
     */
    private Optional<DataMapper<? super T, ?>> getParentMapper(){
        Class<? super T> aClass = type.getSuperclass();
        if(aClass != Object.class && DomainObject.class.isAssignableFrom(aClass)) {
            DataMapper<? super T, ?> classMapper = MapperRegistry.getMapper((Class<DomainObject>) aClass);
            return Optional.of(classMapper);
        }
        return Optional.empty();
    }

    @Override
    public void insert(T obj) {
        SqlFunction<PreparedStatement, T> func = stmt -> setVersion(stmt, obj);
        func = func.compose(stmt -> {
            boolean anyMatch = mapperSettings
                    .getIds()
                    .stream()
                    .anyMatch(sqlFieldId1 -> sqlFieldId1.identity);

            //If there's an ID generated by the DB, get the generatedKey
            if(anyMatch){
                SqlConsumer<Field> fieldSqlConsumer = field -> {
                    field.setAccessible(true);
                    field.set(obj, SQLUtils.getGeneratedKey(stmt));
                };

                mapperSettings
                        .getIds()
                        .stream()
                        .map(sqlFieldId -> sqlFieldId.field)
                        .findFirst()
                        .ifPresent(fieldSqlConsumer.wrap());
            }
            return stmt;
        });

        //This is only done once because it will be recursive (The parentClass will check if its parentClass is a DomainObject and therefore call its insert)
        getParentMapper().ifPresent(objectDataMapper -> objectDataMapper.insert(obj));
        //If the id is autogenerated, it will be set on the obj by the insert of the parent

        SQLUtils.execute(mapperSettings.getInsertQuery(), stmt -> {
            boolean noneMatch = mapperSettings
                    .getIds()
                    .stream()
                    .noneMatch(f -> f.identity);

            if(noneMatch)
                setIds(stmt, obj.getIdentityKey(), 0);

            int offset = (int) mapperSettings
                    .getIds()
                    .stream()
                    .filter(sqlFieldId -> !sqlFieldId.identity)
                    .count();
            setColumns(stmt, obj, offset);
        })
                .thenApply(func.wrap())
                .thenAccept(t -> identityMap.put(t.getIdentityKey(), obj))
                .join();
    }

    private T setVersion(PreparedStatement stmt, T obj) throws SQLException, IllegalAccessException {
        if(stmt.getUpdateCount()==0) throw new ConcurrencyException("Concurrency problem found");
        Field version = null;
        try {
            version = type.getDeclaredField("version");
            version.setAccessible(true);
            version.set(obj, SQLUtils.getVersion(stmt));
        } catch (NoSuchFieldException ignored) { logger.info("version field not found on " + type.getSimpleName()); }

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
                    getParentMapper().ifPresent(objectDataMapper -> objectDataMapper.update(obj));
                    return t;
                })
                .thenAccept(o -> {
                    if(!tryReplace(o, 5000)) throw new ConcurrencyException("Concurrency problem found, could not update IdentityMap");
                }).join();
    }

    @Override
    public void delete(T obj) {
        SQLUtils.execute(mapperSettings.getDeleteQuery(), stmt -> setIds(stmt, obj.getIdentityKey(), 0))
                .thenAccept(s -> getParentMapper().ifPresent(objectDataMapper -> objectDataMapper.delete(obj)))
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
