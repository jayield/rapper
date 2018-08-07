package com.github.jayield.rapper.mapper.externals;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.mapper.MapperRegistry;
import com.github.jayield.rapper.mapper.MapperSettings;
import com.github.jayield.rapper.sql.SqlFieldExternal;
import com.github.jayield.rapper.unitofwork.UnitOfWork;
import com.github.jayield.rapper.utils.*;
import com.github.jayield.rapper.mapper.MapperRegistry.Container;

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
                .map(sqlFieldId -> getPrimaryKeyValue(t, sqlFieldId.getField()));
    }

    /**
     * Will call the external object's mapper's find with T's ids to find the external objects who are referenced by T
     *
     * @param t
     * @param sqlFieldExternal
     * @param container
     * @param idValues
     */
    @Override
    public <N extends DomainObject<V>, V> void populate(T t, SqlFieldExternal sqlFieldExternal, Container<N, V> container, Stream<Object> idValues) {
        Iterator<Object> idValues1 = idValues.iterator();
        EqualCondition<Object>[] pairs = Arrays.stream(sqlFieldExternal.getForeignNames())
                .map(str -> new EqualCondition<>(str, idValues1.next()))
                .toArray(EqualCondition[]::new);

        Function<UnitOfWork, CompletableFuture<List<N>>> objects = unit -> MapperRegistry.getMapper(sqlFieldExternal.getDomainObjectType(), unit).find(pairs);

        try {
            sqlFieldExternal.getField().setAccessible(true);
            sqlFieldExternal.getField().set(t, objects);
        } catch (IllegalAccessException e) {
            throw new DataMapperException(e);
        }
    }



}
