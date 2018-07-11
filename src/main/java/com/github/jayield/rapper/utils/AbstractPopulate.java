package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.EmbeddedId;
import com.github.jayield.rapper.ExternalsHandler;
import com.github.jayield.rapper.exceptions.DataMapperException;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public abstract class AbstractPopulate<T extends DomainObject<K>, K> implements Populate<T> {
    protected final ExternalsHandler<T, K> externalsHandler;
    protected final MapperSettings mapperSettings;

    public AbstractPopulate(ExternalsHandler<T,K> externalsHandler, MapperSettings mapperSettings){
        this.externalsHandler = externalsHandler;
        this.mapperSettings = mapperSettings;
    }

    @Override
    public CompletableFuture<Void> execute(T t, SqlField.SqlFieldExternal sqlFieldExternal) {
        populate(t, sqlFieldExternal, MapperRegistry.getContainer(sqlFieldExternal.domainObjectType), idValues(t, sqlFieldExternal));
        return null;
    }

    public abstract Stream<Object> idValues(T t, SqlField.SqlFieldExternal sqlFieldExternal);
    public abstract<N extends DomainObject<V>,V> void populate(T t, SqlField.SqlFieldExternal sqlFieldExternal, MapperRegistry.Container<N, V> container, Stream<Object> idValues);


    /**
     * Sets the field of T with the List passed in the parameters
     * The field must be a collection or a Supplier
     *
     * @param t
     * @param futureSupplier
     * @param field
     * @throws DataMapperException
     */
    /*protected <R> void setExternal(T t, Function<UnitOfWork, CompletableFuture<R>> futureSupplier, Field field) {
        try {
            field.setAccessible(true);
            field.set(t, futureSupplier);
        } catch (IllegalAccessException e) {
            throw new DataMapperException(e);
        }
    }
*/
    /**
     * It will get the value of the primary key from t
     *
     * @param t
     * @param field
     * @return
     */
    protected Object getPrimaryKeyValue(T t, Field field) {
        try {
            field.setAccessible(true);
            if (mapperSettings.getPrimaryKeyConstructor() == null)
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
}
