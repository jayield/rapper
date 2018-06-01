package com.github.jayield.rapper;

import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.github.jayield.rapper.utils.SqlField.*;

public class DataMapper<T extends DomainObject<K>, K> implements Mapper<T, K> {

    private static final String QUERY_ERROR = "Couldn't execute query on {}.\nReason: {}";
    private static final int ITEMS_IN_PAGE = 10;

    private final Class<T> type;
    private final Class<?> primaryKeyType;
    private final Constructor<T> constructor;
    private final Constructor<?> primaryKeyConstructor;
    private final Logger log = LoggerFactory.getLogger(DataMapper.class);
    private final MapperSettings mapperSettings;

    public DataMapper(Class<T> type, MapperSettings mapperSettings) {
        this.type = type;
        this.mapperSettings = mapperSettings;

        primaryKeyType = mapperSettings.getPrimaryKeyType();
        primaryKeyConstructor = mapperSettings.getPrimaryKeyConstructor();

        try {
            constructor = type.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new DataMapperException(e);
        }
    }

    @Override
    public CompletableFuture<Long> getNumberOfEntries() {
        return SQLUtils.execute(String.format("select COUNT(*) as c from %s", type.getSimpleName()), s -> { })
                .thenApply(this::getNumberOfEntries);
    }

    private Long getNumberOfEntries(PreparedStatement ps) {
        try (ResultSet rs = ps.getResultSet()) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new DataMapperException(e);
        }
    }

    @Override
    public <R> CompletableFuture<List<T>> findWhere(Pair<String, R>... values) {
        return findWhereAux("", values);
    }

    @Override
    public <R> CompletableFuture<List<T>> findWhere(int page, int numberOfItems, Pair<String, R>... values) {
        return findWhereAux(String.format(mapperSettings.getPagination(), page*numberOfItems, numberOfItems), values);
    }

    @Override
    public CompletableFuture<Optional<T>> findById(K id) {
        UnitOfWork unitOfWork = UnitOfWork.getCurrent();
        String selectByIdQuery = mapperSettings.getSelectByIdQuery();
        return SQLUtils.execute(selectByIdQuery, stmt -> SQLUtils.setValuesInStatement(mapperSettings.getIds().stream(), stmt, id))
                .thenApply(ps -> {
                    UnitOfWork.setCurrent(unitOfWork);
                    log.info("Queried database for {} with id {}", type.getSimpleName(), id);
                    return getStream(ps).findFirst();
                })
                .exceptionally(throwable -> {
                    log.warn(QUERY_ERROR, type.getSimpleName(), throwable.getMessage());
                    throw new DataMapperException(throwable);
                });
    }

    @Override
    public CompletableFuture<List<T>> findAll() {
        return findAllAux("");
    }

    @Override
    public CompletableFuture<List<T>> findAll(int page, int numberOfItems) {
        return findAllAux(String.format(mapperSettings.getPagination(), page*numberOfItems, numberOfItems));
    }

    @Override
    public CompletableFuture<Void> create(T obj) {
        UnitOfWork current = UnitOfWork.getCurrent();
        Optional<Mapper<? super T, ? super K>> parentMapper = getParentMapper();

        if (parentMapper.isPresent()) {
            return parentMapper
                    .get()
                    .create(obj)
                    .thenCompose(ignored -> createAux(obj, current));
        } else return createAux(obj, current);
    }

    @Override
    public CompletableFuture<Void> createAll(Iterable<T> t) {
        return reduceCompletableFutures(t, this::create);
    }

    @Override
    public CompletableFuture<Void> update(T obj) {
        UnitOfWork current = UnitOfWork.getCurrent();
        Optional<Mapper<? super T, ? super K>> parentMapper = getParentMapper();

        if (parentMapper.isPresent()) {
            return parentMapper
                    .get()
                    .update(obj)
                    .thenCompose(ignored -> updateAux(obj, current));
        } else return updateAux(obj, current);
    }

    @Override
    public CompletableFuture<Void> updateAll(Iterable<T> t) {
        return reduceCompletableFutures(t, this::update);
    }

    @Override
    public CompletableFuture<Void> deleteById(K k) {
        UnitOfWork unit = UnitOfWork.getCurrent();
        return SQLUtils.execute(mapperSettings.getDeleteQuery(), stmt -> SQLUtils.setValuesInStatement(mapperSettings.getIds().stream(), stmt, k))
                .thenCompose(preparedStatement -> {
                    UnitOfWork.setCurrent(unit);
                    log.info("Deleted {} with id {}", type.getSimpleName(), k);
                    return getParentMapper()
                            .map(objectDataMapper -> objectDataMapper.deleteById(k))
                            .orElse(CompletableFuture.completedFuture(null));
                })
                .exceptionally(throwable -> {
                    log.warn("Couldn't deleteById {}. \nReason: {}", type.getSimpleName(), throwable.getMessage());
                    throw new DataMapperException(throwable);
                });
    }

    @Override
    public CompletableFuture<Void> delete(T obj) {
        UnitOfWork unit = UnitOfWork.getCurrent();
        return SQLUtils.execute(mapperSettings.getDeleteQuery(), stmt ->
                SQLUtils.setValuesInStatement(mapperSettings.getIds().stream(), stmt, obj)
        )
                .thenCompose(preparedStatement -> {
                    UnitOfWork.setCurrent(unit);
                    log.info("Deleted {} with id {}", type.getSimpleName(), obj.getIdentityKey());
                    return getParentMapper()
                            .map(objectDataMapper -> objectDataMapper.delete(obj))
                            .orElse(CompletableFuture.completedFuture(null));
                })
                .exceptionally(throwable -> {
                    log.warn("Couldn't delete {}. \nReason: {}", type.getSimpleName(), throwable.getMessage());
                    throw new DataMapperException(throwable);
                });
    }

    @Override
    public CompletableFuture<Void> deleteAll(Iterable<K> keys) {
        return reduceCompletableFutures(keys, this::deleteById);
    }

    private <R> CompletableFuture<List<T>> findWhereAux(String suffix, Pair<String, R>... values){
        String query = Arrays.stream(values)
                .map(p -> p.getKey() + " = ? ")
                .collect(Collectors.joining(" AND ", mapperSettings.getSelectQuery() + " WHERE ", suffix));

        return SQLUtils.execute(query, stmt -> prepareFindWhere(stmt, values))
                .thenApply(preparedStatement -> processFindWhere(preparedStatement, values))
                .exceptionally(throwable -> {
                    log.info(QUERY_ERROR, type.getSimpleName(), throwable.getMessage());
                    throw new DataMapperException(throwable);
                });
    }

    private <R> List<T> processFindWhere(PreparedStatement preparedStatement, Pair<String, R>[] values) {
        StringBuilder sb = new StringBuilder("Queried database for ");
        sb.append(type.getSimpleName()).append(" where ");
        for (int i = 0; i < values.length; i++) {
            sb.append(values[i].getKey()).append(" = ").append(values[i].getValue());
            if (i + 1 < values.length) sb.append(" and ");
        }
        log.info(sb.toString());
        return getStream(preparedStatement).collect(Collectors.toList());
    }

    private <R> void prepareFindWhere(PreparedStatement stmt, Pair<String, R>[] values) {
        try {
            for (int i = 0; i < values.length; i++) {
                stmt.setObject(i + 1, values[i].getValue());
            }
        } catch (SQLException e) {
            throw new DataMapperException(e);
        }
    }

    private CompletableFuture<List<T>> findAllAux(String suffix) {
        return SQLUtils.execute(mapperSettings.getSelectQuery() + suffix, s -> { })
                .thenApply(preparedStatement -> {
                    log.info("Queried database for all objects of type {}", type.getSimpleName());
                    return getStream(preparedStatement).collect(Collectors.toList());
                })
                .exceptionally(throwable -> {
                    log.info(QUERY_ERROR, type.getSimpleName(), throwable.getMessage());
                    throw new DataMapperException(throwable);
                });
    }

    private CompletableFuture<Void> createAux(T obj, UnitOfWork current) {
        UnitOfWork.setCurrent(current);
        return SQLUtils.execute(mapperSettings.getInsertQuery(), stmt -> prepareCreate(obj, stmt))
                .thenCompose(ps -> processCreate(obj, current, ps))
                .thenAccept(result -> {
                    result.ifPresent(item -> setVersion(obj, item.getVersion()));
                    log.info("Create new {}", type.getSimpleName());
                })
                .exceptionally(throwable -> {
                    log.warn("Couldn't create {}. \nReason: {}", type.getSimpleName(), throwable.getMessage());
                    throw new DataMapperException(throwable);
                });
    }

    private CompletionStage<Optional<T>> processCreate(T obj, UnitOfWork current, PreparedStatement ps) {
        try {
            UnitOfWork.setCurrent(current);
            ResultSet rs = ps.getGeneratedKeys();
            setGeneratedKeys(obj, rs);
            return this.findById(obj.getIdentityKey());
        } catch (SQLException e) {
            throw new DataMapperException(e);
        }
    }

    private void prepareCreate(T obj, PreparedStatement stmt) {
        Stream<SqlFieldId> ids = mapperSettings
                .getIds()
                .stream()
                .filter(sqlFieldId -> !sqlFieldId.identity || sqlFieldId.isFromParent());
        Stream<SqlField> columns = mapperSettings
                .getColumns()
                .stream();
        Stream<SqlFieldExternal> externals = mapperSettings
                .getExternals()
                .stream()
                .filter(sqlFieldExternal -> sqlFieldExternal.names.length != 0);

        Stream<SqlField> fields = Stream.concat(Stream.concat(ids, columns), externals)
                .sorted(Comparator.comparing(SqlField::byInsert));

        SQLUtils.setValuesInStatement(fields, stmt, obj);
    }

    private CompletableFuture<Void> updateAux(T obj, UnitOfWork current) {
        UnitOfWork.setCurrent(current);
        return SQLUtils.execute(mapperSettings.getUpdateQuery(), stmt -> prepareUpdate(obj, stmt))
                .thenCompose(ps -> processUpdate(obj, current, ps))
                .thenAccept(result -> {
                    result.ifPresent(item -> setVersion(obj, item.getVersion()));
                    log.info("Updated {} with id {}", type.getSimpleName(), obj.getIdentityKey());
                })
                .exceptionally(throwable -> {
                    log.warn("Couldn't update {}. \nReason: {}", type.getSimpleName(), throwable.getMessage());
                    throw new DataMapperException(throwable);
                });
    }

    private CompletableFuture<Optional<T>> processUpdate(T obj, UnitOfWork current, PreparedStatement ps) {
        try {
            UnitOfWork.setCurrent(current);
            int updateCount = ps.getUpdateCount();
            if (updateCount == 0) throw new DataMapperException("No rows affected by update, object's version might be wrong");
            return this.findById(obj.getIdentityKey());
        } catch (SQLException e) {
            throw new DataMapperException(e);
        }
    }

    private void prepareUpdate(T obj, PreparedStatement stmt) {
        try {
            Stream<SqlFieldId> ids = mapperSettings.getIds().stream();
            Stream<SqlField> columns = mapperSettings.getColumns().stream();
            Stream<SqlFieldExternal> externals = mapperSettings
                    .getExternals()
                    .stream()
                    .filter(sqlFieldExternal -> sqlFieldExternal.names.length != 0);

            Stream<SqlField> fields = Stream.concat(Stream.concat(ids, columns), externals)
                    .sorted(Comparator.comparing(SqlField::byUpdate));

            SQLUtils.setValuesInStatement(fields, stmt, obj);

            //Since each object has its own version, we want the version from type not from the subClass
            SqlFieldVersion versionField = mapperSettings.getVersionField();
            if(versionField != null) {
                Field f = versionField.field;
                f.setAccessible(true);
                long version = (long) f.get(obj);

                long externalsSize = mapperSettings
                        .getExternals()
                        .stream()
                        .filter(sqlFieldExternal -> sqlFieldExternal.names.length != 0)
                        .flatMap(sqlFieldExternal -> Arrays.stream(sqlFieldExternal.names))
                        .count();

                stmt.setLong((int) (mapperSettings.getColumns().size() + mapperSettings.getIds().size() + externalsSize + 1), version);
            }
        } catch (SQLException | IllegalAccessException e) {
            throw new DataMapperException(e);
        }
    }

    private <R> CompletableFuture<Void> reduceCompletableFutures(Iterable<R> r, Function<R, CompletableFuture<Void>> function) {
        List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
        r.forEach(k -> completableFutures.add(function.apply(k)));
        return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]));
    }

    private Stream<T> stream(Statement statement, ResultSet rs) {
        return StreamSupport.stream(new Spliterators.AbstractSpliterator<T>(
                Long.MAX_VALUE, Spliterator.ORDERED) {
            @Override
            public boolean tryAdvance(Consumer<? super T> action) {
                try {
                    if (!rs.next()) return false;
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
        });
    }

    private T mapper(ResultSet rs) {
        try {
            T t = constructor.newInstance();
            Object primaryKey = primaryKeyConstructor != null ? primaryKeyConstructor.newInstance() : null;

            //Set t's primary key field to primaryKey if its a composed primary key
            if (primaryKey != null) {
                Arrays.stream(type.getDeclaredFields())
                        .filter(field -> field.getType() == this.primaryKeyType)
                        .findFirst()
                        .ifPresent(field -> {
                            try {
                                field.setAccessible(true);
                                field.set(t, primaryKey);
                            } catch (IllegalAccessException e) {
                                throw new DataMapperException(e);
                            }
                        });
            }

            List<Object> idValues = new ArrayList<>();

            mapperSettings
                    .getAllFields()
                    .stream()
                    .filter(field -> mapperSettings.getFieldPredicate().test(field.field))
                    .forEach(sqlField -> setField(rs, t, primaryKey, sqlField, idValues));

            if(primaryKey != null) {
                //!! DON'T FORGET TO SET VALUES ON "objects" FIELD ON EMBEDDED ID CLASS !!>
                EmbeddedIdClass.getObjectsField().set(primaryKey, idValues.toArray());
            }

            return t;
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new DataMapperException(e);
        }
    }

    private void setField(ResultSet rs, T t, Object primaryKey, SqlField sqlField, List<Object> idValues) {
        Field field = sqlField.field;
        String name = sqlField.name;
        try {
            field.setAccessible(true);
            //Get the Id from foreign table if the field annotated with ColumnName has NAME defined
            if(name.equals(SQL_FIELD_EXTERNAL)){
                SqlFieldExternal sqlFieldExternal = (SqlFieldExternal) sqlField;

                String[] names = sqlFieldExternal.names;
                Object[] objects = new Object[names.length];
                for (int i = 0; i < names.length; i++) {
                    String s = names[i];
                    objects[i] = rs.getObject(s);
                }

                sqlFieldExternal.setIdValues(objects);
            }
            else
                field.set(t, rs.getObject(name));
        } catch (IllegalArgumentException e) { //If IllegalArgumentException is caught, is because field is from primaryKeyClass
            try {
                if (primaryKey != null) {
                    Object object = rs.getObject(name);
                    idValues.add(object);
                    field.set(primaryKey, object);
                }
                else
                    field.set(t, rs.getObject(name, field.getType()));
                /*
                 * This "if else" is done because jdbc might not convert to the right type in the first rs.getObject, but in the second it will
                 * Ex: for a field of type "short" rs.getObject(name, field.getType()) will return null, but rs.getObject(name) will return an Integer
                 * if we execute first rs.getObject(name) and then rs.getObject(name, field.getType()), it will return a "short" value...
                 */
            } catch (SQLException | IllegalAccessException e1) {
                throw new DataMapperException(e1);
            }
        } catch (SQLException | IllegalAccessException e) {
            throw new DataMapperException(e);
        }
    }

    /**
     * Gets the mapper of the parent of T.
     * If the parent implements DomainObject (meaning it has a mapper), gets its mapper, else returns an empty Optional.
     *
     * @return Optional of DataMapper or empty Optional
     */
    private Optional<Mapper<? super T, ? super K>> getParentMapper() {
        Class<? super T> aClass = type.getSuperclass();
        if (aClass != Object.class && DomainObject.class.isAssignableFrom(aClass)) {
            Mapper<? super T, ? super K> classMapper = MapperRegistry.getRepository((Class<DomainObject>) aClass).getMapper();
            return Optional.of(classMapper);
        }
        return Optional.empty();
    }

    /**
     * rs.next() should be called before calling this method
     *
     * @param obj
     * @param rs
     */
    private void setGeneratedKeys(T obj, ResultSet rs) {
        mapperSettings
                .getIds()
                .stream()
                .filter(f -> f.identity && !f.isFromParent())
                .forEach(field -> {
                    try {
                        rs.next();
                        field.field.setAccessible(true);
                        String url = ConnectionManager.getConnectionManager().getUrl();
                        if (url.toLowerCase().contains("sqlserver")) {
                            BigDecimal bigDecimal = rs.getBigDecimal(1);
                            Object key;

                            if (field.field.getType() == Integer.class)
                                key = bigDecimal.intValue();
                            else key = bigDecimal.longValue();

                            field.field.set(obj, key);
                        } else
                            field.field.set(obj, rs.getObject(1, field.field.getType()));   //Doesn't work on sqlServer, the type of GeneratedKey is bigDecimal always
                    } catch (IllegalAccessException | SQLException e) {
                        throw new DataMapperException(e);
                    }
                });
    }

    private void setVersion(T obj, long newValue) {
        try {
            SqlFieldVersion versionField = mapperSettings.getVersionField();
            if(versionField != null) {
                Field version = versionField.field;
                version.setAccessible(true);
                version.set(obj, newValue);
            }
        } catch (IllegalAccessException ignored) {
            log.info("Version field not found on {}.", type.getSimpleName());
        }
    }

    private Stream<T> getStream(PreparedStatement s) {
        try {
            return stream(s, s.getResultSet());
        } catch (SQLException e) {
            throw new DataMapperException(e);
        }
    }
}
