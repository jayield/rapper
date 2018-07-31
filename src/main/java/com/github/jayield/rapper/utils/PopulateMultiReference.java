package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.ExternalsHandler;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.utils.MapperRegistry.Container;
import com.github.jayield.rapper.utils.SqlField.SqlFieldExternal;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class PopulateMultiReference<T extends DomainObject<K>, K> extends AbstractPopulate<T, K> {

    public PopulateMultiReference(ExternalsHandler<T, K> externalsHandler, MapperSettings mapperSettings) {
        super(externalsHandler, mapperSettings);
    }

    @Override
    public Stream<Object> idValues(T t, SqlFieldExternal sqlFieldExternal) {
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
    public <N extends DomainObject<V>, V> void populate(T t, SqlFieldExternal sqlFieldExternal, Container<N, V> container, Stream<Object> idValues, UnitOfWork unit) {
        Iterator<Object> idValues1 = idValues.iterator();
        Pair<String, Object>[] pairs = Arrays.stream(sqlFieldExternal.foreignNames)
                .map(str -> new Pair<>(str, idValues1.next()))
                .toArray(Pair[]::new);

        Supplier<CompletableFuture<List<N>>> objects = () -> MapperRegistry.getRepository(sqlFieldExternal.domainObjectType, unit).findWhere(pairs);

        try {
            sqlFieldExternal.field.setAccessible(true);
            sqlFieldExternal.field.set(t, objects);
        } catch (IllegalAccessException e) {
            throw new DataMapperException(e);
        }
    }



}
