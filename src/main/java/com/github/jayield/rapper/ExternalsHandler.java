package com.github.jayield.rapper;

import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.utils.*;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.jayield.rapper.utils.SqlField.SqlFieldExternal;
import static com.github.jayield.rapper.utils.SqlField.SqlFieldId;

public class ExternalsHandler<T extends DomainObject<K>, K> {

    private final Constructor<?> primaryKeyConstructor;
    private final Field[] primaryKeyDeclaredFields;
    private final HashMap<Class<? extends Populate>, Populate<T>> map;
    private final Logger logger = LoggerFactory.getLogger(ExternalsHandler.class);
    private final List<SqlFieldExternal> externals;
    private final List<SqlFieldId> ids;

    public ExternalsHandler(MapperSettings mapperSettings) {
        this.externals = mapperSettings.getExternals();
        this.primaryKeyConstructor = mapperSettings.getPrimaryKeyConstructor();

        ids = mapperSettings.getIds();
        Class<?> primaryKey = mapperSettings.getPrimaryKeyType();
        primaryKeyDeclaredFields = primaryKey != null ? primaryKey.getDeclaredFields() : null;

        map = new HashMap<>();

        map.put(PopulateSingleReference.class, new PopulateSingleReference<>(this));
        map.put(PopulateMultiReference.class, new PopulateMultiReference<>(this));
        map.put(PopulateWithExternalTable.class, new PopulateWithExternalTable<>(this));
    }

    /**
     * Will set the fields marked with @ColumnName by querying the database
     *
     * @param t
     */
    void populateExternals(T t) {
        if (externals != null) externals
                .forEach(sqlFieldExternal -> {
                    Populate<T> populate = map.get(sqlFieldExternal.getPopulateStrategy());
                    if(populate == null)
                        throw new DataMapperException("The annotation ColumnName didn't follow the rules");
                    populate.execute(t, sqlFieldExternal);
                });
    }

    /**
     * It will get the external object's Ids and call its mapper to obtain all external objects
     *
     * @param <V>
     * @param repo
     * @param foreignNames
     * @param resultSet
     * @param unit
     * @return
     */
    public <N extends DomainObject<V>, V> Stream<CompletableFuture<N>> getExternalObjects(DataRepository<N, V> repo, String[] foreignNames, ResultSet resultSet, UnitOfWork unit) {
        List<V> idValues = getIds(resultSet, foreignNames)
                .stream()
                .map(e -> (V)e)
                .collect(Collectors.toList());
        return idValues
                .stream()
                .map(v -> repo.findById(unit, v))
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
    private List<Object> getIds(io.vertx.ext.sql.ResultSet rs, String[] foreignNames) {
        SqlFunction<JsonObject, Stream<Object>> function;

        if (primaryKeyConstructor == null)
            function = jo -> Arrays.stream(foreignNames).map(n -> {
                //System.out.println(jo);
                Object o = jo.getValue(n);
                //System.out.println(o);
                return o;
            });
        else
            function = jo -> {
                List<Object> idValues = new ArrayList<>();
                Object newInstance = primaryKeyConstructor.newInstance();
                for (int i = 0; i < foreignNames.length; i++) {
                    Object object = jo.getValue(foreignNames[i]);
                    idValues.add(object);
                    primaryKeyDeclaredFields[i].set(newInstance, object);
                }
                //!! DON'T FORGET TO SET VALUES ON "objects" FIELD ON EMBEDDED ID CLASS !!>
                EmbeddedIdClass.getObjectsField().set(newInstance, idValues.toArray());
                return Stream.of(newInstance);
            };

        return rs.getRows(true).stream().flatMap(function.wrap()).collect(Collectors.toList());
    }

    // Go to externals check if they have a SingleExternalReference,
    // if they do, go to that reference, check if they have the same reference in opposite way
    // and if they do, add on that list the object
    public CompletableFuture<Void> insertReferences(T obj) {
        return changeReferences(obj, true);
    }

    public CompletableFuture<Void> updateReferences(T prevDomainObj, T obj) {
        if (externals == null) return CompletableFuture.completedFuture(null);

        return CompletableFuture.allOf(externals.stream()
                .filter(sqlFieldExternal -> sqlFieldExternal.getPopulateStrategy() != PopulateMultiReference.class)
                .map(sqlFieldExternal -> {
                    try {
                        sqlFieldExternal.field.setAccessible(true);
                        CompletableFuture prevExternalCF = (CompletableFuture) sqlFieldExternal.field.get(prevDomainObj);
                        CompletableFuture externalCF = (CompletableFuture) sqlFieldExternal.field.get(obj);

                        List<SqlFieldExternal> domainObjectExternals = MapperRegistry.getMapperSettings(sqlFieldExternal.domainObjectType).getExternals();

                        //external might be a list, in case of externalTable or a DomainObject in case a singleReference

                        /*
                         * Cases:
                         * 1º New Reference - prevExternalCF = null && externalCF = newValue
                         * 2º Update Reference - prevExternalCF = value && externalCF = newValue
                         * 3º Remove Reference - prevExternalCF = value && externalCF = null
                         * 4º Do nothing - prevExternalCF = null && externalCF = null
                         */

                        /*if (prevExternalCF == null && externalCF != null) {
                            changeCFReferences(obj, externalCF, domainObjectExternals, true);
                        } else if (prevExternalCF != null && externalCF != null) {
                            changeCFReferences(obj, prevExternalCF, domainObjectExternals, false);
                            changeCFReferences(obj, externalCF, domainObjectExternals, true);
                        } else if (prevExternalCF != null) {
                            changeCFReferences(obj, prevExternalCF, domainObjectExternals, false);
                        }*/
                        List<CompletableFuture> futures = new ArrayList<>();

                        if (externalCF != null) futures.add(changeCFReferences(obj, externalCF, domainObjectExternals, true));
                        if (prevExternalCF != null) futures.add(changeCFReferences(obj, prevExternalCF, domainObjectExternals, false));

                        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
                    } catch (IllegalAccessException e) {
                        throw new DataMapperException(e);
                    }
                })
                .toArray(CompletableFuture[]::new));
    }

    public CompletableFuture<Void> removeReferences(T obj) {
        return changeReferences(obj, false);
    }

    private CompletableFuture<Void> changeCFReferences(T obj, CompletableFuture externalCF, List<SqlFieldExternal> externals, boolean isToStore) {
        return externalCF.thenAccept(external -> {
            if (List.class.isAssignableFrom(external.getClass()))
                ((List<Object>) external).forEach(e -> modifyList(obj, isToStore, e, externals));
            else
                modifyList(obj, isToStore, external, externals);
        });
    }

    private CompletableFuture<Void> changeReferences(T obj, boolean isToStore) {
        if (externals == null) return CompletableFuture.completedFuture(null);

        return CompletableFuture.allOf(externals.stream()
                .filter(sqlFieldExternal -> sqlFieldExternal.getPopulateStrategy() != PopulateMultiReference.class)
                .map(sqlFieldExternal -> {
                    try {
                        sqlFieldExternal.field.setAccessible(true);
                        CompletableFuture externalCF = (CompletableFuture) sqlFieldExternal.field.get(obj);

                        List<SqlFieldExternal> domainObjectExternals = MapperRegistry.getMapperSettings(sqlFieldExternal.domainObjectType).getExternals();

                        //external might be a list, in case of externalTable or a DomainObject in case a singleReference
                        if (externalCF != null) return changeCFReferences(obj, externalCF, domainObjectExternals, isToStore);
                        return CompletableFuture.completedFuture(null);
                    } catch (IllegalAccessException e) {
                        throw new DataMapperException(e);
                    }
                })
                .toArray(CompletableFuture[]::new));
    }

    /**
     * @param obj The object to add or remove from the list
     * @param isToStore flag to indicate if it should add new object
     * @param external the external object to obtain the value of the completablefuture
     * @param externalExternals A list with fields of external from the External object
     */
    private void modifyList(T obj, boolean isToStore, Object external, List<SqlFieldExternal> externalExternals) {
        Optional<SqlFieldExternal> optionalFutureField = externalExternals
                .stream()
                .filter(sqlFieldExternal -> sqlFieldExternal.domainObjectType == obj.getClass())
                .findFirst();

        optionalFutureField.ifPresent(sqlFieldExternal -> {
            try {
                Field field = sqlFieldExternal.field;
                field.setAccessible(true);

                /*
                * Since multiple threads might try to alter field's value, it must be under a lock.
                * To avoid only having a monitor for all fields, each field will have its own monitor.
                */
                synchronized (sqlFieldExternal.mon){
                    CompletableFuture<? extends List<T>> completableFuture = (CompletableFuture<? extends List<T>>) field.get(external);
                    if (completableFuture != null) {
                        CompletableFuture<? extends List<T>> future = completableFuture
                                .thenApply(list -> {
                                    list.removeIf(t -> t.getIdentityKey().equals(obj.getIdentityKey()));
                                    if (isToStore) list.add(obj);
                                    return list;
                                });

                        field.set(external, future);
                    }
                }
            } catch (IllegalAccessException e) {
                throw new DataMapperException(e);
            }
        });
    }

    public List<SqlFieldId> getIds() {
        return ids;
    }

    public Constructor<?> getPrimaryKeyConstructor() {
        return primaryKeyConstructor;
    }
}
