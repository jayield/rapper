package com.github.jayield.rapper;

import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.utils.*;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.github.jayield.rapper.utils.SqlField.*;

public class DataMapper<T extends DomainObject<K>, K> implements Mapper<T, K> {

    private static final String QUERY_ERROR = "Couldn't execute query on {}.\nReason: {}";

    private final Logger log = LoggerFactory.getLogger(DataMapper.class);
    private final Class<T> type;
    private final MapperSettings mapperSettings;
    private final Constructor<T> constructor;
    private final ExternalsHandler<T, K> externalHandler;

    private Class<?> primaryKeyType = null;
    private Constructor<?> primaryKeyConstructor = null;

    public DataMapper(Class<T> type) {
        this.type = type;
        try {
            Class<?>[] declaredClasses = type.getDeclaredClasses();
            if (declaredClasses.length > 0) {
                primaryKeyType = declaredClasses[0]; //TODO change, don't get it from declaredClasses
                primaryKeyConstructor = primaryKeyType.getConstructor();
            }
            constructor = type.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new DataMapperException(e);
        }
        mapperSettings = new MapperSettings(type);
        externalHandler = new ExternalsHandler<>(mapperSettings.getIds(), mapperSettings.getExternals(), primaryKeyType, primaryKeyConstructor);
    }

    @Override
    public <R> CompletableFuture<List<T>> findWhere(Pair<String, R>... values) {
        UnitOfWork current = UnitOfWork.getCurrent();
        String query = Arrays.stream(values)
                .map(p -> p.getKey() + " = ? ")
                .collect(Collectors.joining(" AND ", mapperSettings.getSelectQuery() + " WHERE ", ""));

        return SQLUtils.execute(query, s -> {
            try {
                for (int i = 0; i < values.length; i++) {
                    s.setObject(i + 1, values[i].getValue());
                }
            } catch (SQLException e) {
                throw new DataMapperException(e);
            }
        })
                .thenApply(this::getStream)
                .thenApply(tStream -> {
                    UnitOfWork.setCurrent(current);
                    return tStream.peek(externalHandler::populateExternals);
                })
                .thenApply(s -> s.collect(Collectors.toList()))
                .exceptionally(throwable -> {
                    log.info(QUERY_ERROR, type.getSimpleName(), throwable.getMessage());
                    //throwable.printStackTrace();
                    return Collections.emptyList();
                });
    }

    @Override
    public CompletableFuture<Optional<T>> findById(K id) {
        UnitOfWork unitOfWork = UnitOfWork.getCurrent();
        return SQLUtils.execute(mapperSettings.getSelectByIdQuery(), stmt -> SQLUtils.setValuesInStatement(mapperSettings.getIds().stream(), stmt, id))
                .thenApply(ps -> {
                    UnitOfWork.setCurrent(unitOfWork);
                    Optional<T> optionalT = getStream(ps).findFirst();
                    optionalT.ifPresent(externalHandler::populateExternals);
                    return optionalT;
                })
                .exceptionally(throwable -> {
                    log.info(QUERY_ERROR, type.getSimpleName(), throwable.getMessage());
                    //throwable.printStackTrace();
                    return Optional.empty();
                });
    }

    @Override
    public CompletableFuture<List<T>> findAll() {
        UnitOfWork current = UnitOfWork.getCurrent();
        return SQLUtils.execute(mapperSettings.getSelectQuery(), s -> {
        })
                .thenApply(this::getStream)
                .thenApply(tStream -> {
                    UnitOfWork.setCurrent(current);
                    return tStream.peek(externalHandler::populateExternals);
                })
                .thenApply(tStream1 -> tStream1.collect(Collectors.toList()))
                .exceptionally(throwable -> {
                    log.info(QUERY_ERROR, type.getSimpleName(), throwable.getMessage());
                    //throwable.printStackTrace();
                    return Collections.emptyList();
                });
    }

    @Override
    public CompletableFuture<Boolean> create(T obj) {
        //This is only done once because it will be recursive (The parentClass will check if its parentClass is a DomainObject and therefore call its insert)
        boolean[] parentSuccess = {true};
        getParentMapper().ifPresent(objectDataMapper -> parentSuccess[0] = objectDataMapper.create(obj).join());
        //If the id is autogenerated, it will be set on the obj by the insert of the parent

        if (!parentSuccess[0]) return CompletableFuture.completedFuture(parentSuccess[0]);

        return SQLUtils.execute(mapperSettings.getInsertQuery(),
                stmt -> {
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
        )
                .thenApply(ps -> {
                    try {
                        ResultSet rs = ps.getResultSet();
                        setVersion(obj, rs);
                        setGeneratedKeys(obj, rs);
                        return true;
                    } catch (SQLException e) {
                        throw new DataMapperException(e);
                    }
                })
                .exceptionally(throwable -> {
                    log.info("Couldn't create {}. \nReason: {}", type.getSimpleName(), throwable.getMessage());
                    return false;
                });
    }

    @Override
    public CompletableFuture<Boolean> createAll(Iterable<T> t) {
        return reduceCompletableFutures(t, this::create);
    }

    @Override
    public CompletableFuture<Boolean> update(T obj) {
        //Updates parents first
        boolean[] parentSuccess = {true};
        getParentMapper().ifPresent(objectDataMapper -> parentSuccess[0] = objectDataMapper.update(obj).join());

        if (!parentSuccess[0]) return CompletableFuture.completedFuture(parentSuccess[0]);

        return SQLUtils.execute(mapperSettings.getUpdateQuery(), stmt -> {
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
                Field f = type.getDeclaredField("version");
                f.setAccessible(true);
                long version = (long) f.get(obj);

                long externalsSize = mapperSettings
                        .getExternals()
                        .stream()
                        .filter(sqlFieldExternal -> sqlFieldExternal.names.length != 0)
                        .flatMap(sqlFieldExternal -> Arrays.stream(sqlFieldExternal.names))
                        .count();

                stmt.setLong((int) (mapperSettings.getColumns().size() + mapperSettings.getIds().size() + externalsSize + 1), version);
            } catch (SQLException | IllegalAccessException | NoSuchFieldException e) {
                throw new DataMapperException(e);
            }
        })
                .thenApply(ps -> {
                    try {
                        setVersion(obj, ps.getResultSet());
                        return true;
                    } catch (SQLException e) {
                        throw new DataMapperException(e);
                    }
                })
                .exceptionally(throwable -> {
                    log.info("Couldn't update {}. \nReason: {}", type.getSimpleName(), throwable.getMessage());
                    return false;
                });
    }

    @Override
    public CompletableFuture<Boolean> updateAll(Iterable<T> t) {
        return reduceCompletableFutures(t, this::update);
    }

    @Override
    public CompletableFuture<Boolean> deleteById(K k) {
        UnitOfWork unit = UnitOfWork.getCurrent();
        return SQLUtils.execute(mapperSettings.getDeleteQuery(), stmt ->
                SQLUtils.setValuesInStatement(mapperSettings.getIds().stream(), stmt, k)
        )
                .thenCompose(preparedStatement -> {
                    UnitOfWork.setCurrent(unit);
                    return getParentMapper()
                            .map(objectDataMapper -> objectDataMapper.deleteById(k))
                            .orElse(CompletableFuture.completedFuture(true));
                })
                .exceptionally(throwable -> {
                    log.info("Couldn't deleteById {}. \nReason: {}", type.getSimpleName(), throwable.getMessage());
                    return false;
                });
    }

    @Override
    public CompletableFuture<Boolean> delete(T obj) {
        UnitOfWork unit = UnitOfWork.getCurrent();
        return SQLUtils.execute(mapperSettings.getDeleteQuery(), stmt ->
                SQLUtils.setValuesInStatement(mapperSettings.getIds().stream(), stmt, obj)
        )
                .thenCompose(preparedStatement -> {
                    UnitOfWork.setCurrent(unit);
                    return getParentMapper()
                            .map(objectDataMapper -> objectDataMapper.delete(obj))
                            .orElse(CompletableFuture.completedFuture(true));
                })
                .exceptionally(throwable -> {
                    log.info("Couldn't delete {}. \nReason: {}", type.getSimpleName(), throwable.getMessage());
                    return false;
                });
    }

    @Override
    public CompletableFuture<Boolean> deleteAll(Iterable<K> keys) {
        return reduceCompletableFutures(keys, this::deleteById);
    }

    private <R> CompletableFuture<Boolean> reduceCompletableFutures(Iterable<R> r, Function<R, CompletableFuture<Boolean>> function) {
        List<CompletableFuture<Boolean>> completableFutures = new ArrayList<>();
        r.forEach(k -> completableFutures.add(function.apply(k)));
        return completableFutures
                .stream()
                .reduce(CompletableFuture.completedFuture(true), (a, b) -> a.thenCombine(b, (a2, b2) -> a2 && b2));
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

            mapperSettings
                    .getAllFields()
                    .stream()
                    .filter(field -> mapperSettings.getFieldPredicate().test(field.field))
                    .forEach(sqlField -> setField(rs, t, primaryKey, sqlField));

            return t;
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new DataMapperException(e);
        }
    }

    private void setField(ResultSet rs, T t, Object primaryKey, SqlField sqlField) {
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
                if (primaryKey != null)
                    field.set(primaryKey, rs.getObject(name));
                else
                    throw new DataMapperException(e);
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
                        field.field.setAccessible(true);
                        field.field.set(obj, rs.getObject(field.name));
                    } catch (IllegalAccessException | SQLException e) {
                        throw new DataMapperException(e);
                    }
                });
    }

    private void setVersion(T obj, ResultSet rs) {
        try {
            if (rs.next()) {
                Field version;

                version = type.getDeclaredField("version");
                version.setAccessible(true);
                version.set(obj, rs.getLong("version"));

            } else throw new DataMapperException("Couldn't get version.");
        } catch (NoSuchFieldException | IllegalAccessException ignored) {
            log.info("Version field not found on {}.", type.getSimpleName());
        } catch (SQLException e) {
            log.info("Couldn't set version on {}.\nReason: {}", type.getSimpleName(), e.getMessage());
        }
    }

    private Stream<T> getStream(PreparedStatement s) {
        try {
            return stream(s, s.getResultSet());
        } catch (SQLException e) {
            throw new DataMapperException(e);
        }
    }

    String getSelectQuery() {
        return mapperSettings.getSelectQuery();
    }

    String getInsertQuery() {
        return mapperSettings.getInsertQuery();
    }

    String getUpdateQuery() {
        return mapperSettings.getUpdateQuery();
    }

    String getDeleteQuery() {
        return mapperSettings.getDeleteQuery();
    }

    Constructor<?> getPrimaryKeyConstructor() {
        return primaryKeyConstructor;
    }

    Class<?> getPrimaryKeyType() {
        return primaryKeyType;
    }

    public MapperSettings getMapperSettings() {
        return mapperSettings;
    }
}