package com.github.jayield.rapper.mapper.externals;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.mapper.MapperRegistry;
import com.github.jayield.rapper.mapper.MapperSettings;
import com.github.jayield.rapper.mapper.conditions.EqualAndCondition;
import com.github.jayield.rapper.sql.SqlFieldExternal;
import com.github.jayield.rapper.unitofwork.UnitOfWork;
import com.github.jayield.rapper.mapper.MapperRegistry.Container;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Stream;

public class PopulateMultiReference<T extends DomainObject<K>, K> extends AbstractPopulate<T, K> {

    public PopulateMultiReference(ExternalsHandler<T, K> externalsHandler, MapperSettings mapperSettings) {
        super(externalsHandler, mapperSettings);
    }

    /**
     * Will call the external object's mapper's find with T's ids to find the external objects who are referenced by T
     *
     * @param t
     * @param sqlFieldExternal
     * @param container
     */
    @Override
    public <N extends DomainObject<V>, V> void populate(T t, SqlFieldExternal sqlFieldExternal, Container<N, V> container) {
        Iterator<Object> idValues = mapperSettings.getIds()
                .stream()
                .map(sqlFieldId -> getPrimaryKeyValue(t, sqlFieldId.getField()))
                .iterator();

        EqualAndCondition<Object>[] pairs = Arrays.stream(sqlFieldExternal.getForeignNames())
                .map(str -> new EqualAndCondition<>(str, idValues.next()))
                .toArray(EqualAndCondition[]::new);

        Function<UnitOfWork, CompletableFuture<List<N>>> objects = unit -> MapperRegistry.getMapper(sqlFieldExternal.getDomainObjectType(), unit).find(pairs);

        try {
            sqlFieldExternal.getField().setAccessible(true);
            sqlFieldExternal.getField().set(t, objects);
        } catch (IllegalAccessException e) {
            throw new DataMapperException(e);
        }
    }



}
