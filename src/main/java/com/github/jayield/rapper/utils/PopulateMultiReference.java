package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.DataRepository;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.ExternalsHandler;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public class PopulateMultiReference<T extends DomainObject<K>, K> extends AbstractPopulate<T, K> {

    public PopulateMultiReference(ExternalsHandler<T, K> externalsHandler, MapperSettings mapperSettings) {
        super(externalsHandler, mapperSettings);
    }

    @Override
    public Stream<Object> idValues(T t, SqlField.SqlFieldExternal sqlFieldExternal) {
        return mapperSettings.getIds()
                .stream()
                .map(sqlFieldId -> getPrimaryKeyValue(t, sqlFieldId.field));
    }

    /**
     * Will call the external object's mapper's findWhere with T's ids to find the external objects who are referenced by T
     *
     * @param t
     * @param sqlFieldExternal
     * @param container
     * @param idValues
     */
    @Override
    public <N extends DomainObject<V>, V> void populate(T t, SqlField.SqlFieldExternal sqlFieldExternal, MapperRegistry.Container<N, V> container, Stream<Object> idValues) {
        Iterator<Object> idValues1 = idValues.iterator();
        Pair<String, Object>[] pairs = Arrays.stream(sqlFieldExternal.foreignNames)
                .map(str -> new Pair<>(str, idValues1.next()))
                .toArray(Pair[]::new);

        UnitOfWork unit = new UnitOfWork(ConnectionManager.getConnectionManager()::getConnection);

        CompletableFuture<? extends List<? extends DomainObject>> objects = container.getDataRepository().findWhere(unit, pairs);

        setExternal(t, objects, sqlFieldExternal.field, sqlFieldExternal.type);
    }



}
