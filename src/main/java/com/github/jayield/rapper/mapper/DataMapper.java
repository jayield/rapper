package com.github.jayield.rapper.mapper;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.mapper.conditions.*;
import com.github.jayield.rapper.mapper.externals.ExternalsHandler;
import com.github.jayield.rapper.sql.SqlField;
import com.github.jayield.rapper.sql.SqlFieldExternal;
import com.github.jayield.rapper.sql.SqlFieldId;
import com.github.jayield.rapper.sql.SqlFieldVersion;
import com.github.jayield.rapper.utils.SqlUtils;
import com.github.jayield.rapper.unitofwork.UnitOfWork;
import com.github.jayield.rapper.utils.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.UpdateResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.jayield.rapper.connections.ConnectionManager.getConnectionManager;
import static com.github.jayield.rapper.sql.SqlField.*;

public class DataMapper<T extends DomainObject<K>, K> implements Mapper<T, K> {
    private static final String QUERY_ERROR = "Couldn't execute {} on {} on Unit of Work {} due to {}";
    private static final Logger logger = LoggerFactory.getLogger(DataMapper.class);

    private final Class<T> type;
    private final ExternalsHandler<T, K> externalsHandler;
    private final MapperSettings mapperSettings;
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private final UnitOfWork unit;
    private final Comparator<T> comparator;

    public DataMapper(Class<T> type, ExternalsHandler<T, K> externalsHandler, MapperSettings mapperSettings, Comparator<T> comparator, UnitOfWork unit) {
        this.type = type;
        this.externalsHandler = externalsHandler;
        this.mapperSettings = mapperSettings;
        this.comparator = comparator;
        this.unit = unit;
    }

    @Override
    public CompletableFuture<Long> getNumberOfEntries(Condition<?>... values) {
        Query query = new Query(mapperSettings.getSelectCountQuery(), values);

        return SqlUtils.query(query.getQueryString(), unit, prepareFind(values))
                .thenApply(this::getNumberOfEntries);
    }

    @Override
    public CompletableFuture<Long> getNumberOfEntries() {
        return SqlUtils.query(mapperSettings.getSelectCountQuery(), unit, new JsonArray())
                .thenApply(this::getNumberOfEntries);
    }

    private Long getNumberOfEntries(ResultSet rs) {
        return rs.getResults().get(0).getLong(0);
    }

    @Override
    public CompletableFuture<List<T>> find(Condition<?>... values) {
        Query query = new Query(mapperSettings.getSelectQuery(), values);

        CompletableFuture<List<T>> future = findAux(query, values);
        return processNewObjects(future);
    }

    @Override
    public CompletableFuture<List<T>> find(int page, int numberOfItems, Condition<?>... values) {
        Query query = new Query(mapperSettings.getSelectQuery(), page, numberOfItems, values);

        CompletableFuture<List<T>> future = findAux(query, values);
        return processNewObjects(future);
    }

    @Override
    public CompletableFuture<Optional<T>> findById(K id) {
        CompletableFuture<T> completableFuture = unit
                .getIdentityMap(type)
                .computeIfAbsent(id, k1 -> findByIdAux(id))
                .thenApply(t -> (T) t);

        return completableFuture
                .thenApply(Optional::of)
                .exceptionally(throwable -> {
                    logger.warn("Removing CompletableFuture of {} from identityMap due to {}", type.getSimpleName(), throwable.getMessage());
                    unit.getIdentityMap(type).remove(id);
                    return Optional.empty();
                });
    }

    private CompletableFuture<T> findByIdAux(K id) {
        String selectByIdQuery = mapperSettings.getSelectByIdQuery();
        return SqlUtils.query(selectByIdQuery, unit, SqlUtils.getValuesForStatement(mapperSettings.getIds().stream(), id))
                .thenApply(rs -> {
                    logger.info("Queried database for {} with id {} with Unit of Work {}", type.getSimpleName(), id, unit.hashCode());
                    Optional<T> optionalT = stream(rs).findFirst();
                    optionalT.ifPresent(this::handleExternals);
                    return optionalT;
                })
                .exceptionally(throwable -> {
                    logger.warn(QUERY_ERROR, "FindById", type.getSimpleName(), unit.hashCode(), throwable.getMessage());
                    throw new DataMapperException(throwable);
                })
                .thenApply(t -> t.orElseThrow(() -> new DataMapperException(type.getSimpleName() + " was not found")));
    }

    @Override
    public CompletableFuture<Void> create(T obj) {
        unit.registerNew(obj);
        Optional<DataMapper<? super T, ? super K>> parentMapper = getParentMapper();

        return parentMapper.map(parent -> parent.create(obj))
                .orElse(CompletableFuture.completedFuture(null))
                .thenCompose(ignored -> createAux(obj));
    }

    @Override
    public CompletableFuture<Void> createAll(Iterable<T> t) {
        return reduceCompletableFutures(t, this::create);
    }

    @Override
    public CompletableFuture<Void> update(T obj) {
        unit.registerDirty(obj);
        Optional<DataMapper<? super T, ? super K>> parentMapper = getParentMapper();

        if (parentMapper.isPresent()) {
            return parentMapper
                    .get()
                    .update(obj)
                    .thenCompose(ignored -> updateAux(obj));
        } else
            return updateAux(obj);
    }

    @Override
    public CompletableFuture<Void> updateAll(Iterable<T> t) {
        return reduceCompletableFutures(t, this::update);
    }

    @Override
    public CompletableFuture<Void> deleteById(K k) {
        CompletableFuture<? extends DomainObject> future = unit.getIdentityMap(type).computeIfPresent(k, (key, tCompletableFuture) -> tCompletableFuture.thenApply(t -> {
            unit.registerRemoved(t);
            return t;
        }));

        return future != null ? deleteByIdAux(k) : findById(k)
                .thenCompose(t -> {
                    T t1 = t.orElseThrow(() -> new DataMapperException("Object to delete was not found"));
                    unit.registerRemoved(t1);
                    return deleteByIdAux(k);
                });
    }

    private CompletableFuture<Void> deleteByIdAux(K k) {
        return SqlUtils.update(mapperSettings.getDeleteQuery(), unit, SqlUtils.getValuesForStatement(mapperSettings.getIds().stream(), k))
                .thenCompose(updateResult -> {
                    logger.info("Deleted {} with id {} with Unit of Work {}", type.getSimpleName(), k, unit.hashCode());
                    return getParentMapper()
                            .map(objectDataMapper -> objectDataMapper.deleteById(k))
                            .orElse(CompletableFuture.completedFuture(null));
                })
                .exceptionally(throwable -> {
                    logger.warn("Couldn't deleteById {}. \nReason: {}", type.getSimpleName(), throwable.getMessage());
                    throw new DataMapperException(throwable);
                });
    }

    @Override
    public CompletableFuture<Void> delete(T obj) {
        unit.registerRemoved(obj);
        return SqlUtils.update(mapperSettings.getDeleteQuery(), unit, SqlUtils.getValuesForStatement(mapperSettings.getIds().stream(), obj))
                .thenCompose(updateResult -> {
                    logger.info("Deleted {} with id {} with Unit of Work {}", type.getSimpleName(), obj.getIdentityKey(), unit.hashCode());
                    return getParentMapper()
                            .map(objectDataMapper -> objectDataMapper.delete(obj))
                            .orElse(CompletableFuture.completedFuture(null));
                })
                .exceptionally(throwable -> {
                    logger.warn("Couldn't delete {} due to {}", type.getSimpleName(), throwable.getMessage());
                    throw new DataMapperException(throwable);
                });
    }

    @Override
    public CompletableFuture<Void> deleteAll(Iterable<K> keys) {
        List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
        keys.forEach(k -> completableFutures.add(deleteById(k)));
        return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]));
    }

    private void handleExternals(T t) {
        Optional<DataMapper<? super T, ? super K>> parentMapper = getParentMapper();
        parentMapper.ifPresent(mapper -> mapper.handleExternals(t));
        externalsHandler.populateExternals(t);
    }

    private CompletableFuture<List<T>> findAux(Query query, Condition<?>[] values) {
        JsonArray params = prepareFind(values);
        return SqlUtils.query(query.getQueryString(), unit, params)
                .thenApply(resultSet -> processFind(resultSet, query.getConditions(), params))
                .exceptionally(throwable -> {
                    logger.warn(QUERY_ERROR, "Find", type.getSimpleName(), unit.hashCode(), throwable.getMessage());
                    throw new DataMapperException(throwable);
                });
    }

    private JsonArray prepareFind(Condition<?>[] values) {
        return Arrays.stream(values).map(Condition::getValue).filter(Objects::nonNull).collect(CollectionUtils.toJsonArray());
    }

    private List<T> processFind(ResultSet resultSet, String conditions, JsonArray params) {
        for (int i = 0; i < params.size(); i++) conditions = conditions.replaceFirst("\\?", params.getValue(i).toString());
        logger.info("Queried database for {}{} with Unit of Work {}", type.getSimpleName(), conditions, unit.hashCode());
        return stream(resultSet).peek(this::handleExternals).collect(Collectors.toList());
    }

    private CompletableFuture<Void> createAux(T obj) {
        return SqlUtils.update(mapperSettings.getInsertQuery(), unit, prepareCreate(obj))
                .thenCompose(updateResult -> processCreate(obj, updateResult))
                .thenAccept(result -> {
                    result.ifPresent(item -> setVersion(obj, item.getVersion()));
                    logger.info("Created new {}", type.getSimpleName());
                })
                .exceptionally(throwable -> {
                    logger.warn("Couldn't create {} due to {}", type.getSimpleName(), throwable.getMessage());
                    throw new DataMapperException(throwable);
                });
    }

    private CompletionStage<Optional<T>> processCreate(T obj, UpdateResult ur) {
        setGeneratedKeys(obj, ur.getKeys());
        if (mapperSettings.getVersionField() != null) {
            unit.invalidate(type, obj.getIdentityKey());
            return this.findById(obj.getIdentityKey());
        }
        return CompletableFuture.completedFuture(Optional.of(obj));
    }

    private JsonArray prepareCreate(T obj) {
        Stream<SqlFieldId> ids = mapperSettings
                .getIds()
                .stream()
                .filter(sqlFieldId -> !sqlFieldId.isIdentity() || sqlFieldId.isFromParent());
        Stream<SqlField> columns = mapperSettings
                .getColumns()
                .stream();
        Stream<SqlFieldExternal> externals = mapperSettings
                .getExternals()
                .stream()
                .filter(sqlFieldExternal -> sqlFieldExternal.getNames().length != 0);

        Stream<SqlField> fields = Stream.concat(Stream.concat(ids, columns), externals)
                .sorted(Comparator.comparing(SqlField::byInsert));

        return SqlUtils.getValuesForStatement(fields, obj);
    }

    private CompletableFuture<Void> updateAux(T obj) {
        return SqlUtils.update(mapperSettings.getUpdateQuery(), unit, prepareUpdate(obj))
                .thenCompose(updateResult -> processUpdate(obj, updateResult))
                .thenAccept(result -> {
                    result.ifPresent(item -> setVersion(obj, item.getVersion()));
                    logger.info("Updated {} with id {}", type.getSimpleName(), obj.getIdentityKey());
                })
                .exceptionally(throwable -> {
                    logger.warn("Couldn't update {}. \nReason: {}", type.getSimpleName(), throwable.getMessage());
                    throw new DataMapperException(throwable);
                });
    }

    private CompletableFuture<Optional<T>> processUpdate(T obj, UpdateResult ur) {
        int updateCount = ur.getUpdated();
        if (updateCount == 0) throw new DataMapperException("No rows affected by update, object's version might be wrong");
        if (mapperSettings.getVersionField() != null) {
            unit.invalidate(type, obj.getIdentityKey());
            return this.findById(obj.getIdentityKey());
        }
        return CompletableFuture.completedFuture(Optional.of(obj));
    }

    private JsonArray prepareUpdate(T obj) {
        try {
            Stream<SqlFieldId> ids = mapperSettings.getIds().stream();
            Stream<SqlField> columns = mapperSettings.getColumns().stream();
            Stream<SqlFieldExternal> externals = mapperSettings
                    .getExternals()
                    .stream()
                    .filter(sqlFieldExternal -> sqlFieldExternal.getNames().length != 0);

            Stream<SqlField> fields = Stream.concat(Stream.concat(ids, columns), externals)
                    .sorted(Comparator.comparing(SqlField::byUpdate));

            JsonArray jsonArray = SqlUtils.getValuesForStatement(fields, obj);

            //Since each object has its own version, we want the version from type not from the subClass
            SqlFieldVersion versionField = mapperSettings.getVersionField();
            if(versionField != null) {
                Field f = versionField.getField();
                f.setAccessible(true);
                long version = (long) f.get(obj);
                jsonArray.add(version);
            }
            return jsonArray;
        } catch ( IllegalAccessException e) {
            throw new DataMapperException(e);
        }
    }

    private CompletableFuture<List<T>> processNewObjects(CompletableFuture<List<T>> future) {
        return future.thenApply(list -> unit.processNewObjects(type, list, comparator))
                .thenCompose(CollectionUtils::listToCompletableFuture);
    }

    private <R> CompletableFuture<Void> reduceCompletableFutures(Iterable<R> r, Function<R, CompletableFuture<Void>> function) {
        List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
        r.forEach(k -> completableFutures.add(function.apply(k)));
        return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]));
    }

    private Stream<T> stream(ResultSet rs) {
        return rs.getRows(true).stream().map(this::mapper);
    }

    private T mapper(JsonObject rs) {
        try {
            T t = (T) mapperSettings.getConstructor().newInstance();
            Constructor primaryKeyConstructor = mapperSettings.getPrimaryKeyConstructor();
            Object primaryKey = primaryKeyConstructor != null ? primaryKeyConstructor.newInstance() : null;
            Class<?> primaryKeyType = mapperSettings.getPrimaryKeyType();

            //Set t's primary key field to primaryKey if its a composed primary key
            if (primaryKey != null) {
                Arrays.stream(type.getDeclaredFields())
                        .filter(field -> field.getType() == primaryKeyType)
                        .findFirst()
                        .ifPresent(field -> {
                            try {
                                field.setAccessible(true);
                                field.set(t, primaryKey);
                            } catch (IllegalAccessException e) {
                                throw new DataMapperException(e);
                            }
                        });
            }

            List<Object> idValues = new ArrayList<>();

            mapperSettings.getAllFields()
                    .stream()
                    .filter(field -> mapperSettings.getFieldPredicate().test(field.getField()))
                    .forEach(sqlField -> setField(rs, t, primaryKey, sqlField, idValues));

            if(primaryKey != null) {
                //!! DON'T FORGET TO SET VALUES ON "objects" FIELD ON EMBEDDED ID CLASS !!>
                EmbeddedIdClass.getObjectsField().set(primaryKey, idValues.toArray());
            }

            return t;
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new DataMapperException(e);
        }
    }

    private void setField(JsonObject rs, T t, Object primaryKey, SqlField sqlField, List<Object> idValues) {
        Field field = sqlField.getField();
        String name = sqlField.getName();
        try {
            field.setAccessible(true);
            //Get the Id from foreign table if the field annotated with ColumnName has NAME defined
            if(name.equals(SQL_FIELD_EXTERNAL)){
                SqlFieldExternal sqlFieldExternal = (SqlFieldExternal) sqlField;

                String[] names = sqlFieldExternal.getNames();
                boolean allNull = true;
                Object[] objects = new Object[names.length];
                for (int i = 0; i < names.length; i++) {
                    String s = names[i];
                    objects[i] = rs.getValue(s);
                    if (objects[i] != null) allNull = false;
                }

                sqlFieldExternal.setForeignKey(allNull ? null : objects);
            }
            else
                field.set(t, rs.getValue(name));
        } catch (IllegalArgumentException e) { //If IllegalArgumentException is caught, is because field is from primaryKeyClass
            try {
                field.setAccessible(true);
                if (primaryKey != null) {
                    Object object = rs.getValue(name);
                    idValues.add(object);
                    field.set(primaryKey, object);
                }
                else
                    field.set(t, field.getType() == Instant.class ?  sdf.parse(rs.getString(name)).toInstant() : rs.getValue(name));
                /*
                 * This "if else" is done because sql server jdbc might not convert to the right type in the first rs.getObject, but in the second it will
                 * Ex: for a field of type "short" rs.getObject(name, field.getType()) will return null, but rs.getObject(name) will return an Integer
                 * if we execute first rs.getObject(name) and then rs.getObject(name, field.getType()), it will return a "short" value...
                 */
            } catch (IllegalAccessException | ParseException e1) {
                throw new DataMapperException(e1);
            }
        } catch (IllegalAccessException e) {
            throw new DataMapperException(e);
        }
    }

    /**
     * Gets the mapper of the parent of T.
     * If the parent implements DomainObject (meaning it has a mapper), gets its mapper, else returns an empty Optional.
     *
     * @return Optional of DataMapper or empty Optional
     */
    private Optional<DataMapper<? super T, ? super K>> getParentMapper() {
        Class<? super T> aClass = type.getSuperclass();
        if (aClass != Object.class && DomainObject.class.isAssignableFrom(aClass)) {
            DataMapper<? super T, ? super K> classMapper = MapperRegistry.getMapper((Class<DomainObject>) aClass, unit);
            return Optional.of(classMapper);
        }
        return Optional.empty();
    }

    private void setGeneratedKeys(T obj, JsonArray keys) {
        mapperSettings.getIds()
                .stream()
                .filter(f -> f.isIdentity() && !f.isFromParent())
                .forEach(field -> {
                    try {
                        field.getField().setAccessible(true);

                        String url = getConnectionManager().getUrl();
                        Object value;
                        if (url.toLowerCase().contains("sqlserver"))
                            value = field.getField().getType() == Integer.class ? (Object) keys.getInteger(0) : keys.getLong(0);
                        else
                            value = keys.getValue(0); //Doesn't work on sqlServer, the type of GeneratedKey is always bigDecimal

                        field.getField().set(obj, value);
                    } catch (IllegalAccessException e) {
                        throw new DataMapperException(e);
                    }
                });
    }

    private void setVersion(T obj, long newValue) {
        try {
            SqlFieldVersion versionField = mapperSettings.getVersionField();
            if(versionField != null) {
                Field version = versionField.getField();
                version.setAccessible(true);
                version.set(obj, newValue);
            }
        } catch (IllegalAccessException ignored) {
            logger.info("Version field not found on {}.", type.getSimpleName());
        }
    }

    private class Query {
        private String conditions;
        private String queryString;

        public Query(String selectQuery, int page, int numberOfItems, Condition<?>... values) {
            String suffix = String.format(" offset %d rows fetch next %d rows only", page * numberOfItems, numberOfItems);
            buildQueryString(values, mapperSettings.getPagination(), suffix, selectQuery);
        }

        public Query(String selectQuery, Condition<?>... values) {
            buildQueryString(values, "", "", selectQuery);
        }

        private void buildQueryString(Condition<?>[] values, String pagination, String suffix, String selectQuery) {
            Map<Class<? extends Condition>, List<Condition<?>>> conditionsMap = Arrays.stream(values)
                    .collect(Collectors.groupingBy(Condition::getClass));

            StringBuilder sb = new StringBuilder();

            //Where AND
            List<Condition<?>> whereConditionList = conditionsMap.getOrDefault(Condition.class, new ArrayList<>());
            whereConditionList.addAll(conditionsMap.getOrDefault(EqualAndCondition.class, new ArrayList<>()));
            whereConditionList.addAll(conditionsMap.getOrDefault(LikeCondition.class, new ArrayList<>()));
            sb.append(whereConditionList.isEmpty() ? "" : getWhereCondition(whereConditionList, " AND "));

            //Where OR
            List<Condition<?>> orConditionList = conditionsMap.getOrDefault(EqualOrCondition.class, new ArrayList<>());
            sb.append(orConditionList.isEmpty() ? "" : getWhereCondition(orConditionList, " OR "));

            //Order By
            List<Condition<?>> orderConditionList = conditionsMap.getOrDefault(OrderCondition.class, new ArrayList<>());
            sb.append(orderConditionList.isEmpty() ? pagination : getOrderByCondition(orderConditionList));


            conditions = sb.toString();
            queryString = selectQuery + conditions + suffix;
        }

        private String getOrderByCondition(List<Condition<?>> orderConditionList) {
            return orderConditionList.stream()
                    .map(condition -> String.format("%s %s", condition.getColumnName(), condition.getComparand()))
                    .collect(Collectors.joining(", ", " ORDER BY ", ""));
        }

        private String getWhereCondition(List<Condition<?>> conditionList, String delimeter) {
            return conditionList.stream()
                    .map(condition -> String.format("%s %s ?", condition.getColumnName(), condition.getComparand()))
                    .collect(Collectors.joining(delimeter, " WHERE ", ""));
        }

        public String getConditions() {
            return conditions;
        }

        public String getQueryString() {
            return queryString;
        }
    }
}
