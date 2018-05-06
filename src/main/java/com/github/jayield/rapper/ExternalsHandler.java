package com.github.jayield.rapper;

import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.utils.*;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.jayield.rapper.utils.SqlField.*;

public class ExternalsHandler<T extends DomainObject<K>, K> {

    private final Constructor<?> primaryKeyConstructor;
    private final Field[] primaryKeyDeclaredFields;
    private final HashMap<List<Integer>, BiConsumer<T, SqlFieldExternal>> map;
    private final Logger logger = LoggerFactory.getLogger(ExternalsHandler.class);
    private final List<SqlFieldExternal> externals;

    public ExternalsHandler(MapperSettings mapperSettings) {
        this.externals = mapperSettings.getExternals();
        this.primaryKeyConstructor = mapperSettings.getPrimaryKeyConstructor();

        List<SqlFieldId> ids = mapperSettings.getIds();
        Class<?> primaryKey = mapperSettings.getPrimaryKeyType();
        primaryKeyDeclaredFields = primaryKey != null ? primaryKey.getDeclaredFields() : null;

        map = new HashMap<>();
        map.put(Arrays.asList(1, 0, 0, 0), (t, sqlFieldExternal) -> {
            DataRepository<? extends DomainObject, Object> externalRepo = MapperRegistry.getRepository(sqlFieldExternal.domainObjectType);
            MapperSettings externalMapperSettings = MapperRegistry.getMapperSettings(sqlFieldExternal.domainObjectType);

            Stream<Object> idValues = Arrays.stream(sqlFieldExternal.getIdValues());

            populateSingleReference(t, sqlFieldExternal, externalMapperSettings, externalRepo, idValues.iterator());
        });

        map.put(Arrays.asList(0, 1, 0, 0), (t, sqlFieldExternal) -> {
            DataRepository<? extends DomainObject, ?> externalRepo = MapperRegistry.getRepository(sqlFieldExternal.domainObjectType);

            Stream<Object> idValues = ids
                    .stream()
                    .map(sqlFieldId -> getPrimaryKeyValue(t, sqlFieldId.field));

            populateMultipleReferences(t, sqlFieldExternal, externalRepo, idValues.iterator());
        });

        map.put(Arrays.asList(0, 1, 1, 1), (t, sqlFieldExternal) -> {
            DataRepository<? extends DomainObject, ?> externalRepo = MapperRegistry.getRepository(sqlFieldExternal.domainObjectType);

            Stream<Object> idValues = ids
                    .stream()
                    .map(sqlFieldId -> getPrimaryKeyValue(t, sqlFieldId.field));

            populateWithExternalTable(t, sqlFieldExternal, externalRepo, idValues.iterator());
        });
    }

    /**
     * Will set the fields marked with @ColumnName by querying the database
     *
     * @param t
     */
    void populateExternals(T t) {
        if (externals != null) externals
                .forEach(sqlFieldExternal -> {
                    BiConsumer<T, SqlFieldExternal> biConsumer = map.get(sqlFieldExternal.getValues());
                    if(biConsumer == null)
                        throw new DataMapperException("The annotation ColumnName didn't follow the rules");
                    biConsumer.accept(t, sqlFieldExternal);
                });
    }

    /*private void populateExternal(T t, SqlFieldExternal sqlFieldExternal) {
        BiConsumer<T, SqlFieldExternal> biConsumer = map.get(sqlFieldExternal.getValues());
        if(biConsumer == null)
            throw new DataMapperException("The annotation ColumnName didn't follow the rules");
        biConsumer.accept(t, sqlFieldExternal);
    }*/

    /**
     * This method will populate the CompletableFuture<DomainObject> belonging to T. This shall be called only when T has a single reference to the external.
     * This method will call th external's mapper findById. The id value(s) will be given by when making a query on T, when converting it to in-memory object (mapper method in DataMapper), it will
     * assign the id value(s) to the SqlFieldExternal, that will later be retrieved by sqlFieldExternal.getIdValues()
     *
     * @param t
     * @param sqlFieldExternal
     * @param externalRepo
     * @param idValues
     * @param <V>
     */
    private <V> void populateSingleReference(T t, SqlFieldExternal sqlFieldExternal, MapperSettings externalMapperSettings, DataRepository<? extends DomainObject, V> externalRepo, Iterator<Object> idValues) {
        Object id;
        Constructor<?> externalPrimaryKeyConstructor = externalMapperSettings.getPrimaryKeyConstructor();
        if (externalPrimaryKeyConstructor == null){
            id = idValues.next();
        }
        else {
            try {
                id = externalPrimaryKeyConstructor.newInstance();
                Object[] idValues1 = sqlFieldExternal.getIdValues();
                Field[] declaredFields = externalMapperSettings
                        .getPrimaryKeyType()
                        .getDeclaredFields();
                for (int i = 0; i < idValues1.length; i++) {
                    declaredFields[i].setAccessible(true);
                    declaredFields[i].set(id, idValues1[i]);
                }
                //!! DON'T FORGET TO SET VALUES ON "objects" FIELD ON EMBEDDEDIDCLASS !!
                EmbeddedIdClass.objectsField.set(id, idValues1);
            } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
                throw new DataMapperException(e);
            }
        }

        CompletableFuture<? extends DomainObject> domainObjects = externalRepo
                .findById((V) id)
                .thenApply(domainObject -> domainObject
                        .orElseThrow(() -> new DataMapperException("Couldn't populate externals of " + t.getClass().getSimpleName() + ". \nThe object wasn't found in the DB")));
        setExternal(t, domainObjects, sqlFieldExternal.field, sqlFieldExternal.type);
    }

    /**
     * Will call the external object's mapper's findWhere with T's ids to find the external objects who are referenced by T
     *
     * @param t
     * @param sqlFieldExternal
     * @param repo
     * @param idValues
     */
    private void populateMultipleReferences(T t, SqlFieldExternal sqlFieldExternal, DataRepository<? extends DomainObject, ?> repo, Iterator<Object> idValues) {
        Pair<String, Object>[] pairs = Arrays.stream(sqlFieldExternal.foreignNames)
                .map(str -> new Pair<>(str, idValues.next()))
                .toArray(Pair[]::new);

        setExternal(t, repo.findWhere(pairs), sqlFieldExternal.field, sqlFieldExternal.type);
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
     * @param repo
     * @param idValues
     */
    private <N extends DomainObject<V>, V> void populateWithExternalTable(T t, SqlFieldExternal sqlFieldExternal, DataRepository<N, V> repo, Iterator<Object> idValues) {
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
                        return getExternalObjects(repo, sqlFieldExternal.externalNames, preparedStatement.getResultSet())
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
     * @param repo
     * @param foreignNames
     * @param resultSet
     * @return
     */
    private <N extends DomainObject<V>, V> Stream<CompletableFuture<N>> getExternalObjects(DataRepository<N, V> repo, String[] foreignNames, ResultSet resultSet) {
        List<V> idValues = getIds(resultSet, foreignNames, repo.getKeyType());

        return idValues
                .stream()
                .map(repo::findById)
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
    private <V> List<V> getIds(ResultSet rs, String[] foreignNames, Class<V> type) {
        try {
            List<V> results = new ArrayList<>();

            SqlConsumer<List<V>> consumer;
            if (primaryKeyConstructor == null)
                consumer = list1 -> {
                    for (String foreignName : foreignNames) list1.add(rs.getObject(foreignName, type));
                };
            else consumer = list -> {
                List<Object> idValues = new ArrayList<>();
                Object newInstance = primaryKeyConstructor.newInstance();
                for (int i = 0; i < foreignNames.length; i++) {
                    Object object = rs.getObject(foreignNames[i]);
                    idValues.add(object);
                    primaryKeyDeclaredFields[i].set(newInstance, object);
                }
                //!! DON'T FORGET TO SET VALUES ON "objects" FIELD ON EMBEDDED ID CLASS !!>
                EmbeddedIdClass.objectsField.set(newInstance, idValues.toArray());
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

    // Go to externals check if they have a SingleExternalReference,
    // if they do, go to that reference, check if they have the same reference in opposite way
    // and if they do, add on that list the object
    public void updateReferences(T obj) {
        changeReferences(obj, true);
    }

    public void removeReferences(T obj) {
        changeReferences(obj, false);
    }

    private void changeReferences(T obj, boolean isToReplace) {
        if (externals == null) return;

        externals.forEach(sqlFieldExternal -> {
            if (!sqlFieldExternal.getValues().equals(Arrays.asList(1, 0, 0, 0)) && !sqlFieldExternal.getValues().equals(Arrays.asList(0, 1, 1, 1))) return;
            try {
                sqlFieldExternal.field.setAccessible(true);
                CompletableFuture externalCP = (CompletableFuture) sqlFieldExternal.field.get(obj);

                //Alternative: getExternals through Mapper.
                //MapperRegistry.getMapperSettings(sqlFieldExternal.domainObjectType).getExternals().stream();
                Stream<Field> domainObjectFields = Arrays.stream(sqlFieldExternal.domainObjectType.getDeclaredFields());

                //external might be a list, in case of externalTable or a DomainObject in case a singleReference
                externalCP.thenAccept(external -> {
                    if(List.class.isAssignableFrom(external.getClass()))
                        ((List<Object>) external).forEach(e -> modifyList(obj, isToReplace, e, domainObjectFields));
                    else
                        modifyList(obj, isToReplace, external, domainObjectFields);
                });
            } catch (IllegalAccessException e) {
                throw new DataMapperException(e);
            }
        });
    }

    /**
     *
     * @param obj The object to add or remove from the list
     * @param isToReplace flag to indicate if it's remove
     * @param external the external object to obtain the value of the completablefuture
     * @param fieldStream A stream with fields of external
     */
    private void modifyList(T obj, boolean isToReplace, Object external, Stream<Field> fieldStream) {
        //WARNING!!!!! FIELDS MIGHT NOT HAVE A SINGLE COMPLETABLE FUTURE!!!! CHECK IF IT'S THE ONE WE WANT
        Optional<Field> optionalFutureField = fieldStream
                .filter(field -> field.getType() == CompletableFuture.class)
                .findFirst();

        optionalFutureField.ifPresent(field -> {
            try {
                field.setAccessible(true);
                CompletableFuture<? extends List> listCP = (CompletableFuture<? extends List>) field.get(external);
                listCP.thenAccept(list -> {
                    list.removeIf(t -> ((T) t).getIdentityKey().equals(obj.getIdentityKey()));
                    if (isToReplace) list.add(obj);
                });
            } catch (IllegalAccessException e) {
                throw new DataMapperException(e);
            }
        });
    }
}
