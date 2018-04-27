package com.github.jayield.rapper;

import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.utils.MapperRegistry;
import com.github.jayield.rapper.utils.SQLUtils;
import com.github.jayield.rapper.utils.SqlConsumer;
import com.github.jayield.rapper.utils.SqlField;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ExternalsHandler<T extends DomainObject<K>, K> {

    private final Logger logger = LoggerFactory.getLogger(ExternalsHandler.class);
    private final List<SqlField.SqlFieldId> ids;
    private final List<SqlField.SqlFieldExternal> externals;
    private final Constructor<?> primaryKeyConstructor;
    private final Field[] primaryKeyDeclaredFields;

    ExternalsHandler(List<SqlField.SqlFieldId> ids, List<SqlField.SqlFieldExternal> externals, Class<?> primaryKey, Constructor<?> primaryKeyConstructor) {
        this.ids = ids;
        this.externals = externals;
        this.primaryKeyConstructor = primaryKeyConstructor;
        primaryKeyDeclaredFields = primaryKey != null ? primaryKey.getDeclaredFields() : null;
    }

    /**
     * Will set the fields marked with @ColumnName by querying the database
     *
     * @param t
     */
    CompletableFuture<Boolean> populateExternals(T t) {
        List<CompletableFuture<Boolean>> completableFutures = new ArrayList<>();
        if (externals != null)
            externals.forEach(sqlFieldExternal -> completableFutures.add(populateExternal(t, sqlFieldExternal)));
        return completableFutures
                .stream()
                .reduce(CompletableFuture.completedFuture(true), (a, b) -> a.thenCombine(b, (a2, b2) -> a2 && b2));
    }

    private CompletableFuture<Boolean> populateExternal(T t, SqlField.SqlFieldExternal sqlFieldExternal) {
        try {
            Mapper<? extends DomainObject, ?> externalMapper = MapperRegistry.getRepository(sqlFieldExternal.type).getMapper();

            Stream<Object> idValues = ids
                    .stream()
                    .map(sqlFieldId -> getPrimaryKeyValue(t, sqlFieldId.field));

            return sqlFieldExternal.table.equals(ColumnName.class.getDeclaredMethod("table").getDefaultValue())
                    ? populateWithDataMapper(t, sqlFieldExternal, externalMapper, idValues.iterator())
                    : populateWithExternalTable(t, sqlFieldExternal, externalMapper, idValues.iterator());
        } catch (NoSuchMethodException e) {
            throw new DataMapperException(e);
        }
    }

    /**
     * Will call the external object's mapper's findWhere with T's ids to find the external objects who are referenced by T
     *
     * @param t
     * @param sqlFieldExternal
     * @param mapper
     * @param idValues
     */
    private CompletableFuture<Boolean> populateWithDataMapper(T t, SqlField.SqlFieldExternal sqlFieldExternal, Mapper<? extends DomainObject, ?> mapper, Iterator<Object> idValues) {
        Pair<String, Object>[] pairs = Arrays.stream(sqlFieldExternal.columnsNames)
                .map(str -> new Pair<>(str, idValues.next()))
                .toArray(Pair[]::new);

        return mapper.findWhere(pairs)
                .thenApply(domainObjects -> {
                    setExternal(t, domainObjects, sqlFieldExternal.field, sqlFieldExternal.fType);
                    return true;
                })
                .exceptionally(throwable -> {
                    logger.info("Couldn't populate externals of {}. \nReason: {}", t.getClass().getSimpleName(), throwable.getMessage());
                    return false;
                });
    }

    /**
     * Used when it's a N-N relation.
     * This method will get the generated selectQuery in SqlFieldExternal, to get from the relation table the ids of the external objects.
     * With this, it will call external object's mapper's getById with those ids and create a list with the results.
     * That List will be setted in the SqlFieldExternal
     *
     * @param <V>
     * @param t
     * @param sqlFieldExternal
     * @param mapper
     * @param idValues
     */
    private <V> CompletableFuture<Boolean> populateWithExternalTable(T t, SqlField.SqlFieldExternal sqlFieldExternal, Mapper<? extends DomainObject, V> mapper, Iterator<Object> idValues) {
        return SQLUtils.execute(sqlFieldExternal.selectTableQuery, stmt -> {
            try {
                for (int i = 1; idValues.hasNext(); i++) stmt.setObject(i, idValues.next());
            } catch (SQLException e) {
                throw new DataMapperException(e);
            }
        })
                .thenApply(preparedStatement -> {
                    try {
                        return getExternalObjects(mapper, sqlFieldExternal.foreignNames, preparedStatement.getResultSet());
                    } catch (SQLException e) {
                        throw new DataMapperException(e);
                    }
                })
                .thenApply(domainObjects -> {
                            List<? extends DomainObject> objects = domainObjects
                                    .map(CompletableFuture::join)
                                    .collect(Collectors.toList());
                            setExternal(t, objects, sqlFieldExternal.field, sqlFieldExternal.fType);
                            return true;
                        }
                )
                .exceptionally(throwable -> {
                    logger.info("Couldn't populate externals of {}. \nReason: {}", t.getClass().getSimpleName(), throwable.getMessage());
                    return false;
                });
    }

    /**
     * It will get the value of the primary key from t
     *
     * @param t
     * @param field
     * @return
     */
    private Object getPrimaryKeyValue(T t, Field field) {
        try {
            field.setAccessible(true);
            if (primaryKeyConstructor == null)
                return field.get(t);
            else {
                Field primaryKeyField = Arrays.stream(t.getClass().getDeclaredFields())
                        .filter(f -> f.isAnnotationPresent(EmbeddedId.class))
                        .findFirst()
                        .get();
                primaryKeyField.setAccessible(true);
                Object primaryKey = primaryKeyField.get(t);

                return field.get(primaryKey);
            }
        } catch (IllegalAccessException e) {
            throw new DataMapperException(e);
        }
    }

    /**
     * It will get the external object's Ids and call its mapper to obtain all external objects
     *
     * @param <V>
     * @param mapper
     * @param foreignNames
     * @param resultSet
     * @return
     */
    private <N extends DomainObject<V>, V> Stream<CompletableFuture<N>> getExternalObjects(Mapper<N, V> mapper, String[] foreignNames, ResultSet resultSet) {
        List<V> ids = getIds(resultSet, foreignNames);

        return ids
                .stream()
                .map(mapper::findById)
                .map(optionalCompletableFuture -> optionalCompletableFuture
                        .thenApply(optional -> optional.orElseThrow(
                                () -> new DataMapperException("Couldn't get external object. Its ID was found in the external table, but not on its table"))
                        )
                );
    }

    /**
     * Get external's object ids from external table
     *
     * @param rs
     * @param foreignNames
     * @return
     * @throws SQLException
     */
    private <V> List<V> getIds(ResultSet rs, String[] foreignNames) {
        try {
            List<V> results = new ArrayList<>();

            SqlConsumer<List<V>> consumer;
            if(primaryKeyConstructor == null)
                consumer = list1 -> {
                    for (String foreignName : foreignNames) list1.add((V) rs.getObject(foreignName));
                };
            else consumer = list -> {
                Object newInstance = primaryKeyConstructor.newInstance();
                for (int i = 0; i < foreignNames.length; i++)
                    primaryKeyDeclaredFields[i].set(newInstance, rs.getObject(foreignNames[i]));
                list.add((V) newInstance);
            };

            while (rs.next()) {
                consumer.wrap().accept(results);
            }
            return results;
        } catch (SQLException e) {
            throw new DataMapperException(e);
        }
    }

    /**
     * Sets the field of T with the List passed in the parameters
     * The field must be a collection or a Supplier
     *
     * @param t
     * @param domainObjects
     * @param field
     * @param fieldType
     * @throws DataMapperException
     */
    private void setExternal(T t, List<? extends DomainObject> domainObjects, Field field, Class<?> fieldType) {
        try {
            if (fieldType.isAssignableFrom(Collection.class)) {
                field.setAccessible(true);
                field.set(t, domainObjects);
            } else if (fieldType.isAssignableFrom(Supplier.class)) {
                Supplier<List<? extends DomainObject>> supplier = () -> domainObjects;
                field.setAccessible(true);
                field.set(t, supplier);
            } else throw new DataMapperException("Couldn't set external, unsupported type");
        } catch (IllegalAccessException e) {
            throw new DataMapperException(e);
        }
    }
}
