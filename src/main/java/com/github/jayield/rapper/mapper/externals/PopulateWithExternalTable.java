package com.github.jayield.rapper.mapper.externals;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.mapper.MapperSettings;
import com.github.jayield.rapper.sql.SqlFieldExternal;
import com.github.jayield.rapper.utils.SqlUtils;
import com.github.jayield.rapper.unitofwork.UnitOfWork;
import com.github.jayield.rapper.utils.*;
import com.github.jayield.rapper.mapper.MapperRegistry.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.jayield.rapper.mapper.MapperRegistry.getMapper;

public class PopulateWithExternalTable<T extends DomainObject<K>, K> extends AbstractPopulate<T, K> {

    private final Logger logger = LoggerFactory.getLogger(PopulateWithExternalTable.class);

    public PopulateWithExternalTable(ExternalsHandler<T, K> externalsHandler, MapperSettings mapperSettings) {
        super(externalsHandler, mapperSettings);
    }

    @Override
    public Stream<Object> idValues(T t, SqlFieldExternal sqlFieldExternal) {
        return mapperSettings.getIds()
                .stream()
                .map(sqlFieldId -> getPrimaryKeyValue(t, sqlFieldId.getField()));
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
     * @param container
     * @param idValues
     */
    @Override
    public <N extends DomainObject<V>, V> void populate(T t, SqlFieldExternal sqlFieldExternal, Container<N, V> container, Stream<Object> idValues) {
        Function<UnitOfWork, CompletableFuture<List<N>>> completableFuture = unit -> getExternal(unit, t, sqlFieldExternal, idValues);

        try {
            sqlFieldExternal.getField().setAccessible(true);
            sqlFieldExternal.getField().set(t, completableFuture);
        } catch (IllegalAccessException e) {
            throw new DataMapperException(e);
        }
    }

    private <N extends DomainObject<V>, V> CompletableFuture<List<N>> getExternal(UnitOfWork unit, T t, SqlFieldExternal sqlFieldExternal, Stream<Object> idValues) {
        return SqlUtils.query(sqlFieldExternal.getSelectTableQuery(), unit, idValues.collect(CollectionUtils.toJsonArray()))
                .thenCompose(resultSet -> externalsHandler.getExternalObjects(getMapper((Class<N>)sqlFieldExternal.getDomainObjectType(), unit), sqlFieldExternal.getExternalNames(), resultSet)
                        .collect(Collectors.collectingAndThen(Collectors.toList(), CollectionUtils::listToCompletableFuture))
                )
                .exceptionally(throwable -> {
                    logger.warn("Couldn't populate externals of {} due to {}", t.getClass().getSimpleName(), throwable.getMessage());
                    throw new DataMapperException(throwable);
                });
    }
}
