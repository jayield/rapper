package com.github.jayield.rapper.mapper.externals;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.annotations.EmbeddedId;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.mapper.MapperRegistry;
import com.github.jayield.rapper.mapper.MapperSettings;
import com.github.jayield.rapper.sql.SqlFieldExternal;
import com.github.jayield.rapper.unitofwork.UnitOfWork;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public abstract class AbstractPopulate<T extends DomainObject<K>, K> implements Populate<T> {
    protected final ExternalsHandler<T, K> externalsHandler;
    protected final MapperSettings mapperSettings;

    public AbstractPopulate(ExternalsHandler<T,K> externalsHandler, MapperSettings mapperSettings){
        this.externalsHandler = externalsHandler;
        this.mapperSettings = mapperSettings;
    }

    @Override
    public CompletableFuture<Void> execute(T t, SqlFieldExternal sqlFieldExternal) {
        populate(t, sqlFieldExternal, MapperRegistry.getContainer(sqlFieldExternal.getDomainObjectType()));
        return null;
    }

    public abstract<N extends DomainObject<V>,V> void populate(T t, SqlFieldExternal sqlFieldExternal, MapperRegistry.Container<N, V> container);

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
