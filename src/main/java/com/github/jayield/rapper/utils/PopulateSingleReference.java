package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.DataRepository;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.ExternalsHandler;
import com.github.jayield.rapper.exceptions.DataMapperException;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public class PopulateSingleReference<T extends DomainObject<K>, K> extends AbstractPopulate<T, K>{
    public PopulateSingleReference(ExternalsHandler<T, K> externalsHandler) {
        super(externalsHandler);
    }

    @Override
    public Stream<Object> idValues(T t, SqlField.SqlFieldExternal sqlFieldExternal) {
        return Arrays.stream(sqlFieldExternal.getIdValues());
    }

    /**
     * This method will populate the CompletableFuture<DomainObject> belonging to T. This shall be called only when T has a single reference to the external.
     * This method will call th external's mapper findById. The id value(s) will be given by when making a query on T, when converting it to in-memory object (mapper method in DataMapper), it will
     * assign the id value(s) to the SqlFieldExternal, that will later be retrieved by sqlFieldExternal.getIdValues()
     *
     * @param sqlFieldExternal
     * @param container
     * @param idValues
     * @param <V>
     */
    @Override
    public <N extends DomainObject<V>, V> void populate(T t, SqlField.SqlFieldExternal sqlFieldExternal, MapperRegistry.Container<N, V> container, Stream<Object> idValues) {
        Object id;
        Constructor<?> externalPrimaryKeyConstructor = container.getMapperSettings().getPrimaryKeyConstructor();
        if (externalPrimaryKeyConstructor == null){
            id = idValues.findFirst().get();
        }
        else {
            try {
                id = externalPrimaryKeyConstructor.newInstance();
                Object[] idValues1 = sqlFieldExternal.getIdValues();
                Field[] declaredFields = container.getMapperSettings()
                        .getPrimaryKeyType()
                        .getDeclaredFields();
                for (int i = 0; i < idValues1.length; i++) {
                    declaredFields[i].setAccessible(true);
                    declaredFields[i].set(id, idValues1[i]);
                }
                //!! DON'T FORGET TO SET VALUES ON "objects" FIELD ON EMBEDDEDIDCLASS !!
                EmbeddedIdClass.getObjectsField().set(id, idValues1);
            } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
                throw new DataMapperException(e);
            }
        }

        UnitOfWork unit = new UnitOfWork(ConnectionManager.getConnectionManager()::getConnection);

        CompletableFuture<? extends DomainObject> domainObjects = container.getDataRepository()
                .findById(unit, (V) id)
                .thenApply(domainObject -> domainObject
                        .orElseThrow(() -> new DataMapperException("Couldn't populate externals of " + t.getClass().getSimpleName() + ". The object wasn't found in the DB")));
        setExternal(t, domainObjects, sqlFieldExternal.field, sqlFieldExternal.type);
    }
}
