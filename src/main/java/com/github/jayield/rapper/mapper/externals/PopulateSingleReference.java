package com.github.jayield.rapper.mapper.externals;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.mapper.MapperRegistry;
import com.github.jayield.rapper.mapper.MapperSettings;
import com.github.jayield.rapper.sql.SqlFieldExternal;
import com.github.jayield.rapper.unitofwork.UnitOfWork;
import com.github.jayield.rapper.utils.*;
import com.github.jayield.rapper.mapper.MapperRegistry.Container;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class PopulateSingleReference<T extends DomainObject<K>, K> extends AbstractPopulate<T, K> {
    public PopulateSingleReference(ExternalsHandler<T, K> externalsHandler, MapperSettings mapperSettings) {
        super(externalsHandler, mapperSettings);
    }

    /**
     * This method will populate the CompletableFuture<DomainObject> belonging to T. This shall be called only when T has a single reference to the external.
     * This method will call th external's mapper findById. The id value(s) will be given by when making a query on T, when converting it to in-memory object (mapper method in DataMapper), it will
     * assign the id value(s) to the SqlFieldExternal, that will later be retrieved by sqlFieldExternal.getForeignKey()
     *
     * @param sqlFieldExternal
     * @param container
     * @param <V>
     */
    @Override
    public <N extends DomainObject<V>, V> void populate(T t, SqlFieldExternal sqlFieldExternal, Container<N, V> container) {
        Object id;
        Constructor<?> externalPrimaryKeyConstructor = container.getMapperSettings().getPrimaryKeyConstructor();

        Object[] idValues = sqlFieldExternal.getForeignKey();
        if(idValues == null)
            id = null;
        else if (externalPrimaryKeyConstructor == null) 
            id = idValues[0];
        else {
            try {
                id = externalPrimaryKeyConstructor.newInstance();
                Field[] declaredFields = container.getMapperSettings()
                        .getPrimaryKeyType()
                        .getDeclaredFields();
                for (int i = 0; i < idValues.length; i++) {
                    declaredFields[i].setAccessible(true);
                    declaredFields[i].set(id, idValues[i]);
                }
                //!! DON'T FORGET TO SET VALUES ON "objects" FIELD ON EMBEDDEDIDCLASS !!
                EmbeddedIdClass.getObjectsField().set(id, idValues);
            } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
                throw new DataMapperException(e);
            }
        }

        Foreign<N, V> value = null;
        if (id != null) {
            Function<UnitOfWork, CompletableFuture<N>> futureSupplier = unit -> MapperRegistry.getMapper((Class<N>) sqlFieldExternal.getDomainObjectType(), unit)
                    .findById((V) id)
                    .thenApply(domainObject -> domainObject
                            .orElseThrow(() -> new DataMapperException("Couldn't populate externals of " + t.getClass().getSimpleName() + ". The object wasn't found in the DB")));
            value = new Foreign<>((V) id, futureSupplier);
        }

        try {
            sqlFieldExternal.getField().setAccessible(true);
            sqlFieldExternal.getField().set(t, value);
        } catch (IllegalAccessException e) {
            throw new DataMapperException(e);
        }
    }
}
