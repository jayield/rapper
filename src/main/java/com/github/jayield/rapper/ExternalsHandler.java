package com.github.jayield.rapper;

import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.utils.*;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.jayield.rapper.utils.SqlField.*;

public class ExternalsHandler<T extends DomainObject<K>, K> {

    private final Logger logger = LoggerFactory.getLogger(ExternalsHandler.class);
    private final List<SqlFieldId> ids;
    private final List<SqlFieldExternal> externals;
    private final Constructor<?> primaryKeyConstructor;
    private final Field[] primaryKeyDeclaredFields;

    private final HashMap<List<Integer>, BiConsumer<T, SqlFieldExternal>> map;

    ExternalsHandler(List<SqlFieldId> ids, List<SqlFieldExternal> externals, Class<?> primaryKey, Constructor<?> primaryKeyConstructor) {
        this.ids = ids;
        this.externals = externals;
        this.primaryKeyConstructor = primaryKeyConstructor;
        primaryKeyDeclaredFields = primaryKey != null ? primaryKey.getDeclaredFields() : null;

        map = new HashMap<>();
        map.put(Arrays.asList(1, 0, 0, 0), (t, sqlFieldExternal) -> {
            Mapper<? extends DomainObject, ?> externalMapper = MapperRegistry.getRepository(sqlFieldExternal.domainObjectType).getMapper();

            Stream<Object> idValues = Arrays.stream(sqlFieldExternal.getIdValues());

            populate(t, sqlFieldExternal, externalMapper, idValues.iterator());
        });

        map.put(Arrays.asList(0, 1, 0, 0), (t, sqlFieldExternal) -> {
            Mapper<? extends DomainObject, ?> externalMapper = MapperRegistry.getRepository(sqlFieldExternal.domainObjectType).getMapper();

            Stream<Object> idValues = ids
                    .stream()
                    .map(sqlFieldId -> getPrimaryKeyValue(t, sqlFieldId.field));

            populateWithDataMapper(t, sqlFieldExternal, externalMapper, idValues.iterator());
        });

        map.put(Arrays.asList(0, 1, 1, 1), (t, sqlFieldExternal) -> {
            Mapper<? extends DomainObject, ?> externalMapper = MapperRegistry.getRepository(sqlFieldExternal.domainObjectType).getMapper();

            Stream<Object> idValues = ids
                    .stream()
                    .map(sqlFieldId -> getPrimaryKeyValue(t, sqlFieldId.field));

            populateWithExternalTable(t, sqlFieldExternal, externalMapper, idValues.iterator());
        });
    }

    /**
     * Will set the fields marked with @ColumnName by querying the database
     *
     * @param t
     */
    void populateExternals(T t) {
        if (externals != null) externals
                .forEach(sqlFieldExternal -> populateExternal(t, sqlFieldExternal));
    }

    private void populateExternal(T t, SqlFieldExternal sqlFieldExternal) {
        BiConsumer<T, SqlFieldExternal> biConsumer = map.get(sqlFieldExternal.getValues());
        if(biConsumer == null)
            throw new DataMapperException("The annotation ColumnName didn't follow the rules");
        biConsumer.accept(t, sqlFieldExternal);
    }

    private <V> void populate(T t, SqlFieldExternal sqlFieldExternal, Mapper<? extends DomainObject, V> mapper, Iterator<Object> idValues) {
        Object id;
        DataMapper<?, V> externalMapper = (DataMapper<?, V>) mapper;
        Constructor<?> externalPrimaryKeyConstructor = externalMapper.getPrimaryKeyConstructor();
        if (externalPrimaryKeyConstructor == null){
            id = idValues.next();
        }
        else {
            try {
                id = externalPrimaryKeyConstructor.newInstance();
                Object[] idValues1 = sqlFieldExternal.getIdValues();
                Field[] declaredFields = externalMapper
                        .getPrimaryKeyType()
                        .getDeclaredFields();
                for (int i = 0; i < idValues1.length; i++) {
                    declaredFields[i].setAccessible(true);
                    declaredFields[i].set(id, idValues1[i]);
                }
            } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
                logger.info("Couldn't populate externals of {}. \nReason: {}", t.getClass().getSimpleName(), e.getMessage());
                throw new DataMapperException(e);
            }
        }

        CompletableFuture<? extends DomainObject> domainObjects = mapper
                .findById((V) id)
                .thenApply(domainObject -> domainObject.
                        orElseThrow(() -> new DataMapperException("Couldn't populate externals of " + t.getClass().getSimpleName() + ". \nThe object wasn't found in the DB")));
        setExternal(t, domainObjects, sqlFieldExternal.field, sqlFieldExternal.type);
    }

    /**
     * Will call the external object's mapper's findWhere with T's ids to find the external objects who are referenced by T
     *
     * @param t
     * @param sqlFieldExternal
     * @param mapper
     * @param idValues
     */
    private void populateWithDataMapper(T t, SqlFieldExternal sqlFieldExternal, Mapper<? extends DomainObject, ?> mapper, Iterator<Object> idValues) {
        Pair<String, Object>[] pairs = Arrays.stream(sqlFieldExternal.foreignNames)
                .map(str -> new Pair<>(str, idValues.next()))
                .toArray(Pair[]::new);

        setExternal(t, mapper.findWhere(pairs), sqlFieldExternal.field, sqlFieldExternal.type);
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
    private <N extends DomainObject<V>, V> void populateWithExternalTable(T t, SqlFieldExternal sqlFieldExternal, Mapper<N, V> mapper, Iterator<Object> idValues) {
        UnitOfWork current = UnitOfWork.getCurrent();
        CompletableFuture<List<N>> completableFuture = SQLUtils.execute(sqlFieldExternal.selectTableQuery, stmt -> {
            try {
                for (int i = 1; idValues.hasNext(); i++) stmt.setObject(i, idValues.next());
            } catch (SQLException e) {
                throw new DataMapperException(e);
            }
        })
                .thenCompose(preparedStatement -> {
                    try {
                        UnitOfWork.setCurrent(current);
                        return getExternalObjects(mapper, sqlFieldExternal.externalNames, preparedStatement.getResultSet())
                                .collect(Collectors.collectingAndThen(Collectors.toList(), this::listToCP));
                    } catch (SQLException e) {
                        throw new DataMapperException(e);
                    }
                })
                .exceptionally(throwable -> {
                    logger.info("Couldn't populate externals of {}. \nReason: {}", t.getClass().getSimpleName(), throwable.getMessage());
                    return Collections.emptyList();
                });

        setExternal(t, completableFuture, sqlFieldExternal.field, sqlFieldExternal.type);
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
                        .orElseThrow(() -> new DataMapperException("EmbeddedId field not found on " + t.getClass().getSimpleName()));
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
        List<V> idValues = getIds(resultSet, foreignNames);

        return idValues
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
            if (primaryKeyConstructor == null)
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
     * Converts a List<CompletableFuture<L>> into a CompletableFuture<List<L>>
     *
     * @param futureList
     * @return
     */
    private <L> CompletableFuture<List<L>> listToCP(List<CompletableFuture<L>> futureList) {
        return CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]))
                .thenApply(v -> futureList
                        .stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));
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
    private <N extends DomainObject> void setExternal(T t, Object domainObjects, Field field, Class<?> fieldType) {
        try {
            if (fieldType.isAssignableFrom(CompletableFuture.class)) {
                field.setAccessible(true);
                field.set(t, domainObjects);
            } else throw new DataMapperException("Couldn't set external, unsupported field type");
        } catch (IllegalAccessException e) {
            throw new DataMapperException(e);
        }
    }
}
