package com.github.jayield.rapper.mapper.externals;

import com.github.jayield.rapper.mapper.DataRepository;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.mapper.MapperSettings;
import com.github.jayield.rapper.sql.SqlFunction;
import com.github.jayield.rapper.unitofwork.UnitOfWork;
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

import static com.github.jayield.rapper.sql.SqlField.SqlFieldExternal;

public class ExternalsHandler<T extends DomainObject<K>, K> {
    private final Map<Class<? extends Populate>, Populate<T>> populatorsMap;
    private final Logger logger = LoggerFactory.getLogger(ExternalsHandler.class);
    private final MapperSettings mapperSettings;

    public ExternalsHandler(MapperSettings mapperSettings) {
        this.mapperSettings = mapperSettings;

        populatorsMap = new HashMap<>();
        populatorsMap.put(PopulateSingleReference.class, new PopulateSingleReference<>(this, mapperSettings));
        populatorsMap.put(PopulateMultiReference.class, new PopulateMultiReference<>(this, mapperSettings));
        populatorsMap.put(PopulateWithExternalTable.class, new PopulateWithExternalTable<>(this, mapperSettings));
    }

    /**
     * Will set the fields marked with @ColumnName by querying the database
     *
     * @param t
     */
    public void populateExternals(T t, UnitOfWork unit) {
        List<SqlFieldExternal> externals = mapperSettings.getExternals();
        if (externals != null)
            externals.forEach(sqlFieldExternal -> {
                Populate<T> populate = populatorsMap.get(sqlFieldExternal.getPopulateStrategy());
                if (populate == null)
                    throw new DataMapperException("The annotation ColumnName didn't follow the rules");
                populate.execute(t, sqlFieldExternal, unit);
            });
    }

    /**
     * It will get the external object's Ids and call its mapper to obtain all external objects
     *
     * @param <V>
     * @param repo
     * @param foreignNames
     * @param resultSet
     * @return
     */
    public <N extends DomainObject<V>, V> Stream<CompletableFuture<N>> getExternalObjects(DataRepository<N, V> repo, String[] foreignNames, ResultSet resultSet) {
        List<V> idValues = getIds(resultSet, foreignNames)
                .stream()
                .map(e -> (V)e)
                .collect(Collectors.toList());
        return idValues
                .stream()
                .map(repo::findById)
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

        Constructor primaryKeyConstructor = mapperSettings.getPrimaryKeyConstructor();
        if (primaryKeyConstructor == null)
            function = jo -> Arrays.stream(foreignNames).map(jo::getValue);
        else
            function = jo -> {
                List<Object> idValues = new ArrayList<>();
                Object newInstance = primaryKeyConstructor.newInstance();
                Field[] primaryKeyDeclaredFields = mapperSettings.getPrimaryKeyType().getDeclaredFields();
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
}
