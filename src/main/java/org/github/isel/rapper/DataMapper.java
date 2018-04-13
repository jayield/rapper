package org.github.isel.rapper;

import javafx.util.Pair;
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
    private Class<?> primaryKey = null;
    private Constructor<?> primaryKeyConstructor = null;
    private final MapperSettings mapperSettings;
    private final Constructor<T> constructor;
    private final Logger logger = LoggerFactory.getLogger(DataMapper.class);
    private ExternalsHandler<T, K> externalHandler;

    public MapperSettings getMapperSettings() {
        return mapperSettings;
    }

    public DataMapper(Class<T> type){
        this.type = type;
        try {
            /*Class[] parameterTypes = mapperSettings
                    .getAllFields()
                    .stream()
                    .map(f -> f.field.getType())
                    .toArray(Class[]::new);*/

            Class<?>[] declaredClasses = type.getDeclaredClasses();
            if(declaredClasses.length > 0){
                primaryKey = declaredClasses[0];
                primaryKeyConstructor = primaryKey.getConstructor();
            }

            constructor = type.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        mapperSettings = new MapperSettings(type);
        externalHandler = new ExternalsHandler<>(mapperSettings.getIds(), mapperSettings.getExternals(), primaryKey, primaryKeyConstructor);
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
            Object primaryKey = primaryKeyConstructor != null ? primaryKeyConstructor.newInstance() : null;

            //Set t's primary key field to primaryKey
            if(primaryKey != null) {
                SqlConsumer<Field> fieldConsumer = field -> {
                    field.setAccessible(true);
                    field.set(t, primaryKey);
                };

                Arrays.stream(type.getDeclaredFields())
                        .filter(field -> field.getType() == this.primaryKey)
                        .findFirst()
                        .ifPresent(fieldConsumer.wrap());
            }

            SqlConsumer<SqlField> fieldSetter = f -> {
                f.field.setAccessible(true);
                try{
                    f.field.set(t, rs.getObject(f.name));
                }
                catch (IllegalArgumentException e){ //If IllegalArgumentException is caught, is because field is from primaryKeyClass
                    f.field.set(primaryKey, rs.getObject(f.name));
                }
            };
            mapperSettings
                    .getAllFields()
                    .stream()
                    .filter(field -> mapperSettings.getFieldPredicate().test(field.field))
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

    @Override
    public CompletableFuture<Optional<T>> findById(K id) {
        T obj = identityMap.get(id);
        if(obj != null)
            return CompletableFuture.completedFuture(Optional.of(obj));
        UnitOfWork unitOfWork = UnitOfWork.getCurrent();

        SqlFunction<PreparedStatement, Stream<T>> func = stmt -> stream(stmt, stmt.getResultSet());
        return SQLUtils.execute(mapperSettings.getSelectByIdQuery(), stmt ->
                setValuesInStatement(mapperSettings.getIds().stream(), stmt, id))
                .thenApply(func.wrap())
                .thenApply(Stream::findFirst)
                .thenApply(optionalT -> {
                    UnitOfWork.setCurrent(unitOfWork);
                    optionalT.ifPresent(t -> externalHandler.populateExternals(t).join());
                    return optionalT;
                });
    }

    @Override
    public CompletableFuture<List<T>> findAll() {
        SqlFunction<PreparedStatement, Stream<T>> func = stmt -> stream(stmt, stmt.getResultSet());
        return SQLUtils.execute(mapperSettings.getSelectQuery(), s ->{})
                .thenApply(func.wrap())
                .thenApply(tStream -> tStream.peek(t -> externalHandler.populateExternals(t).join()))
                .thenApply(tStream1 -> tStream1.collect(Collectors.toList()));
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
    public CompletableFuture<Boolean> create(T obj) {
        SqlConsumer<PreparedStatement> consumer = stmt -> setVersion(stmt, obj);
        SqlConsumer<PreparedStatement> func = consumer.compose(stmt -> {
            SqlConsumer<SqlField> fieldSqlConsumer = field -> {
                    field.field.setAccessible(true);
                    field.field.set(obj, SQLUtils.getGeneratedKey(stmt));
                };
            mapperSettings.getIds().stream().filter(f->f.identity).forEach(fieldSqlConsumer.wrap());
            return stmt;
        });

        //This is only done once because it will be recursive (The parentClass will check if its parentClass is a DomainObject and therefore call its insert)
        getParentMapper().ifPresent(objectDataMapper -> objectDataMapper.create(obj).join());
        //If the id is autogenerated, it will be set on the obj by the insert of the parent

        return SQLUtils.execute(mapperSettings.getInsertQuery(), stmt ->
            setValuesInStatement(
                    Stream.concat(mapperSettings.getIds().stream(), mapperSettings.getColumns().stream())
                            .sorted(Comparator.comparing(SqlField::byInsert)), stmt, obj)
        )
                .thenApply(ps ->{
                    func.wrap().accept(ps);
                    return true;
                });
    }

    @Override
    public CompletableFuture<Boolean> createAll(Iterable<T> t) {
        //TODO
        return null;
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

    private void setValuesInStatement(Stream<? extends SqlField> fields, PreparedStatement stmt, Object obj){
        CollectionUtils.zipWithIndex(fields).forEach(entry -> entry.item.setValueInStatement(stmt, entry.index+1, obj));
    }

    @Override
    public CompletableFuture<Boolean> update(T obj) {
        SqlConsumer<PreparedStatement> func = s -> setVersion(s, obj);

        //Updates parents first
        getParentMapper().ifPresent(objectDataMapper -> objectDataMapper.update(obj).join());

        return SQLUtils.execute(mapperSettings.getUpdateQuery(), stmt -> {
            setValuesInStatement(
                    Stream.concat(mapperSettings.getIds().stream(), mapperSettings.getColumns().stream())
                            .sorted(Comparator.comparing(SqlField::byUpdate)), stmt, obj);
            //Since each object has its own version, we want the version from type not from the subClass
            SqlFunction<T, Long> getVersion = t -> {
                Field f = type.getDeclaredField("version");
                f.setAccessible(true);
                return (Long) f.get(t);
            };

            SqlConsumer<PreparedStatement> setVersion = preparedStatement ->
                    preparedStatement.setLong(
                            mapperSettings.getColumns().size() + mapperSettings.getIds().size() + 1,
                            getVersion.wrap().apply(obj)
                    );
            setVersion.wrap().accept(stmt);
        })
                .thenApply(ps -> {
                    func.wrap().accept(ps);
                    return true;
                });
    }

    @Override
    public CompletableFuture<Boolean> updateAll(Iterable<T> t) {
        //TODO
        return null;
    }

    @Override
    public CompletableFuture<Boolean> deleteById(K k) {
        //TODO
        return null;
    }

    @Override
    public CompletableFuture<Boolean> delete(T obj) {
        UnitOfWork unit = UnitOfWork.getCurrent();
        return SQLUtils.execute(mapperSettings.getDeleteQuery(), stmt ->
                setValuesInStatement(mapperSettings.getIds().stream(), stmt, obj)
        )
                .thenCompose(preparedStatement -> {
                    UnitOfWork.setCurrent(unit);
                    return getParentMapper()
                            .map(objectDataMapper -> objectDataMapper.delete(obj))
                            .orElse(CompletableFuture.completedFuture(null));
                });
    }

    @Override
    public CompletableFuture<Boolean> deleteAll(Iterable<K> keys) {
        //TODO
        return null;
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
