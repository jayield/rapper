package org.github.isel.rapper;

import javafx.util.Pair;
import org.github.isel.rapper.exceptions.DataMapperException;
import org.github.isel.rapper.utils.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ExternalsHandler<T extends DomainObject<K>, K> {

    private final List<SqlField.SqlFieldId> ids;
    private final List<SqlField.SqlFieldExternal> externals;
    private final Constructor<?> primaryKeyConstructor;
    private final Field[] primaryKeyDeclaredFields;

    ExternalsHandler(List<SqlField.SqlFieldId> ids, List<SqlField.SqlFieldExternal> externals, Class<?> primaryKey, Constructor<?> primaryKeyConstructor) {
        this.ids = ids;
        this.externals = externals;
        this.primaryKeyConstructor = primaryKeyConstructor;
        primaryKeyDeclaredFields = primaryKey != null ? primaryKey.getDeclaredFields() : null;
    }

    /**
     * Will set the fields marked with @ColumnName by querying the database
     *
     * @param t
     */
    CompletableFuture<Void> populateExternals(T t) {
        List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
        if (externals != null)
            externals.forEach(sqlFieldExternal -> completableFutures.add(populateExternal(t, sqlFieldExternal)));
        return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]));
    }

    private CompletableFuture<Void> populateExternal(T t, SqlField.SqlFieldExternal sqlFieldExternal) {
        try {
            Class<? extends DomainObject> collectionObjectsType = sqlFieldExternal.type;
            Mapper<? extends DomainObject, ?> collectionObjectsTypeMapper = MapperRegistry.getRepository(collectionObjectsType).getMapper();
            String[] columnsNames = sqlFieldExternal.columnsNames;

            Stream<Object> idValues = ids
                    .stream()
                    .map(sqlFieldId -> getPrimaryKeyValue(t, sqlFieldId));

            return sqlFieldExternal.table.equals(ColumnName.class.getDeclaredMethod("table").getDefaultValue())
                    ? populateWithDataMapper(t, sqlFieldExternal, collectionObjectsTypeMapper, columnsNames, idValues.iterator())
                    : populateWithExternalTable(t, sqlFieldExternal, collectionObjectsTypeMapper, idValues.iterator());
        } catch (NoSuchMethodException e) {
            throw new DataMapperException(e);
        }
    }

    /**
     * Will call the external object's mapper's findWhere with T's ids to find the external objects who are referenced by T
     *
     * @param t
     * @param sqlFieldExternal
     * @param mapper
     * @param columnsNames
     * @param idValues
     */
    private CompletableFuture<Void> populateWithDataMapper(T t, SqlField.SqlFieldExternal sqlFieldExternal, Mapper<? extends DomainObject, ?> mapper, String[] columnsNames, Iterator<Object> idValues) {
        Pair<String, Object>[] pairs = Arrays.stream(columnsNames)
                .map(str -> new Pair<>(str, idValues.next()))
                .toArray(Pair[]::new);

        return mapper.findWhere(pairs)
                .thenAccept(domainObjects -> setExternal(t, sqlFieldExternal, domainObjects));
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
     * @param mapper
     * @param idValues
     */
    private <V> CompletableFuture<Void> populateWithExternalTable(T t, SqlField.SqlFieldExternal sqlFieldExternal, Mapper<? extends DomainObject, V> mapper, Iterator<Object> idValues) {
        return SQLUtils.execute(sqlFieldExternal.selectTableQuery, stmt -> {
            try {
                for (int i = 1; idValues.hasNext(); i++) stmt.setObject(i, idValues.next());
            } catch (SQLException e) {
                throw new DataMapperException(e);
            }
        })
                .thenApply(preparedStatement -> getExternalObjects(sqlFieldExternal, mapper, preparedStatement))
                .thenAccept(domainObjects -> setExternal(t, sqlFieldExternal, domainObjects));
    }

    /**
     * It will get the value of the primary key from t
     *
     * @param t
     * @param sqlFieldId
     * @return
     */
    private Object getPrimaryKeyValue(T t, SqlField.SqlFieldId sqlFieldId) {
        try {
            sqlFieldId.field.setAccessible(true);
            if (primaryKeyConstructor == null)
                return sqlFieldId.field.get(t);
            else {
                Field primaryKeyField = Arrays.stream(t.getClass().getDeclaredFields())
                        .filter(field -> field.isAnnotationPresent(EmbeddedId.class))
                        .findFirst()
                        .get();
                primaryKeyField.setAccessible(true);
                Object primaryKey = primaryKeyField.get(t);

                return sqlFieldId.field.get(primaryKey);
            }
        } catch (IllegalAccessException e) {
            throw new DataMapperException(e);
        }
    }

    /**
     * It will get the external object's Ids and call its mapper to obtain all external objects
     *
     * @param <V>
     * @param sqlFieldExternal
     * @param mapper
     * @param preparedStatement
     * @return
     */
    private <N extends DomainObject<V>, V> List<N> getExternalObjects(SqlField.SqlFieldExternal sqlFieldExternal, Mapper<N, V> mapper, PreparedStatement preparedStatement) {
        try {
            List<V> ids = getIds(sqlFieldExternal, preparedStatement.getResultSet());

            return ids
                    .stream()
                    .map(mapper::findById)
                    .map(CompletableFuture::join)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
        } catch (SQLException e) {
            throw new DataMapperException(e);
        }
    }

    /**
     * Get external's object ids from external table
     *
     * @param sqlFieldExternal
     * @param rs
     * @return
     * @throws SQLException
     */
    private <V> List<V> getIds(SqlField.SqlFieldExternal sqlFieldExternal, ResultSet rs) {
        try {
            List<V> results = new ArrayList<>();

            while (rs.next()) {
                //TODO clean code
                if (primaryKeyConstructor == null)
                    for (int i = 0; i < sqlFieldExternal.foreignNames.length; i++)
                        results.add((V) rs.getObject(sqlFieldExternal.foreignNames[i]));
                else {
                    Object newInstance = primaryKeyConstructor.newInstance();
                    for (int i = 0; i < sqlFieldExternal.foreignNames.length; i++)
                        primaryKeyDeclaredFields[i].set(newInstance, rs.getObject(sqlFieldExternal.foreignNames[i]));
                    results.add((V) newInstance);
                }
            }
            return results;
        } catch (IllegalAccessException | SQLException | InvocationTargetException | InstantiationException e) {
            throw new DataMapperException(e);
        }
    }

    /**
     * Sets the field of T with the List passed in the parameters
     * The field must be a collection or a Supplier
     *
     * @param t
     * @param sqlFieldExternal
     * @param domainObjects
     * @throws IllegalAccessException
     */
    private void setExternal(T t, SqlField.SqlFieldExternal sqlFieldExternal, List<? extends DomainObject> domainObjects) {
        try {
            if (sqlFieldExternal.fType.isAssignableFrom(Collection.class)) {
                sqlFieldExternal.field.setAccessible(true);
                sqlFieldExternal.field.set(t, domainObjects);
            } else if (sqlFieldExternal.fType.isAssignableFrom(Supplier.class)) {
                Supplier<List<? extends DomainObject>> supplier = () -> domainObjects;
                sqlFieldExternal.field.setAccessible(true);
                sqlFieldExternal.field.set(t, supplier);
            } else throw new DataMapperException("Couldn't set external, unsupported type");
        } catch (IllegalAccessException e) {
            throw new DataMapperException(e);
        }
    }
}
