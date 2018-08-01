package com.github.jayield.rapper.mapper;

import com.github.jayield.rapper.*;
import com.github.jayield.rapper.annotations.ColumnName;
import com.github.jayield.rapper.annotations.EmbeddedId;
import com.github.jayield.rapper.annotations.Id;
import com.github.jayield.rapper.annotations.Version;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.utils.EmbeddedIdClass;
import com.github.jayield.rapper.mapper.externals.Foreign;
import com.github.jayield.rapper.sql.SqlField;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.jayield.rapper.sql.SqlField.*;

public class MapperSettings {

    private final Class<?> type;

    private List<SqlFieldId> ids = new ArrayList<>();
    private List<SqlField> columns = new ArrayList<>();
    private SqlFieldVersion versionField;
    private List<SqlFieldExternal> externals = new ArrayList<>();
    private List<SqlField> allFields = new ArrayList<>();

    private String selectQuery;
    private String selectCountQuery;
    private String insertQuery;
    private String updateQuery;
    private String deleteQuery;
    private String selectByIdQuery;
    private String pagination;
    private Class<?> primaryKeyType = null;
    private Constructor<?> primaryKeyConstructor;
    private final Constructor<?> constructor;

    private final Predicate<Field> fieldPredicate = field ->
            field.getType().isPrimitive()
            || field.getType().isAssignableFrom(String.class)
            || field.getType().isAssignableFrom(Instant.class)
            || field.getType().isAssignableFrom(CompletableFuture.class)
            || field.getType().isAssignableFrom(Integer.class)
            || field.getType().isAssignableFrom(Double.class)
            || field.getType().isAssignableFrom(Long.class)
            || field.getType().isAssignableFrom(Boolean.class)
            || field.getType().isAssignableFrom(Supplier.class)
            || field.getType().isAssignableFrom(Foreign.class);

    public MapperSettings(Class<?> type) {
        this.type = type;
        try {
            constructor = type.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new DataMapperException(e);
        }

        operations = initOperations(type);

        Map<Class, List<SqlField>> fieldMap = Arrays.stream(type.getDeclaredFields())
                .flatMap(field -> toSqlField(field, "C."))
                .collect(Collectors.groupingBy(SqlField::getClass));

        addParentsFields(type);

        Optional.ofNullable(fieldMap.get(SqlFieldId.class))
                .ifPresent(sqlFields ->
                        ids.addAll(
                                sqlFields
                                        .stream()
                                        .map(f -> ((SqlFieldId) f))
                                        .collect(Collectors.toList())
                        )
                );

        Optional.ofNullable(fieldMap.get(SqlField.class))
                .ifPresent(sqlFields -> columns.addAll(sqlFields));

        Optional.ofNullable(fieldMap.get(SqlFieldVersion.class))
                .ifPresent(sqlFields -> {
                    versionField = (SqlFieldVersion) sqlFields.get(0);
                    columns.addAll(sqlFields);
                });

        Optional.ofNullable(fieldMap.get(SqlFieldExternal.class))
                .ifPresent(sqlFields -> externals = sqlFields.stream().map(f -> (SqlFieldExternal) f).collect(Collectors.toList()));

        allFields.addAll(ids);
        allFields.addAll(columns);
        allFields.addAll(externals);

        buildQueryStrings();
    }

    /**
     * Add ids and fields from parent classes to ids and allFields respectively
     *
     * @param type
     */
    private void addParentsFields(Class<?> type) {
        int[] i = {1};
        for (Class<?> clazz = type.getSuperclass(); clazz != Object.class && DomainObject.class.isAssignableFrom(clazz); clazz = clazz.getSuperclass(), i[0]++) {
            Map<Class, List<SqlField>> parentFieldMap = Arrays.stream(clazz.getDeclaredFields())
                    .flatMap(field -> toSqlField(field, String.format("P%d.", i[0])))
                    .collect(Collectors.groupingBy(SqlField::getClass));

            List<SqlFieldId> sqlFieldIds = parentFieldMap.getOrDefault(SqlFieldId.class, new ArrayList<>())
                    .stream()
                    .map(f -> ((SqlFieldId) f))
                    .peek(SqlFieldId::setFromParent)
                    .collect(Collectors.toList());

            ids.addAll(sqlFieldIds);

            allFields.addAll(parentFieldMap.get(SqlField.class));

            List<SqlField> sqlFieldVersions = parentFieldMap.get(SqlFieldVersion.class);
            if (sqlFieldVersions != null) allFields.addAll(sqlFieldVersions);
        }
    }

    private void buildQueryStrings() {
        List<String> idName = ids
                .stream()
                .map(f -> f.selectQueryValue)
                .collect(Collectors.toList());

        List<String> allFieldsNames = allFields
                .stream()
                //We don't want the externals in our selectQuery, unless NAME from SqlFieldExternal is defined
                .filter(sqlField -> !SqlFieldExternal.class.isAssignableFrom(sqlField.getClass())
                        || SqlFieldExternal.class.isAssignableFrom(sqlField.getClass()) && ((SqlFieldExternal)sqlField).names.length != 0)
                .map(sqlField -> sqlField.selectQueryValue)
                .collect(Collectors.toList());

        StringBuilder suffix = new StringBuilder();
        suffix.append(" from ").append(type.getSimpleName()).append(" C ");

        int[] i = {1};
        for (Class<?> clazz = type.getSuperclass(); clazz != Object.class && DomainObject.class.isAssignableFrom(clazz); clazz = clazz.getSuperclass(), i[0]++) {
            suffix.append("inner join ").append(clazz.getSimpleName()).append(String.format(" P%d ", i[0])).append("on ");

            //Set the comparisons
            for (int j = 0; j < idName.size(); j++) {
                String version = idName.get(j).split("\\.")[1];

                //The previous table's ID
                if (i[0] == 1) suffix.append("C.").append(version);
                else suffix.append(String.format("P%d.", i[0] - 1)).append(version);

                //always the table's ID
                suffix.append(" = ").append(String.format("P%d.", i[0])).append(version).append(" ");
            }
        }

        selectQuery = allFieldsNames
                .stream()
                .collect(Collectors.joining(", ", "select ", suffix));

        selectCountQuery = "select COUNT(*) as c " + suffix;

        selectByIdQuery = selectQuery +
                idName.stream()
                        .map(id -> id + " = ?")
                        .collect(Collectors.joining(" and ", " where ", ""));

        idName = ids
                .stream()
                .map(f -> f.name)
                .collect(Collectors.toList());

        pagination = " order by " + idName.stream().collect(Collectors.joining(", ")) + " offset %d rows fetch next %d rows only";

        List<String> columnsNames = Stream.concat(
                columns
                        .stream()
                        .map(f -> f.name),
                externals
                        .stream()
                        .filter(sqlFieldExternal -> sqlFieldExternal.names.length != 0)
                        .flatMap(sqlFieldExternal -> Arrays.stream(sqlFieldExternal.names))
                ).collect(Collectors.toList());

        columnsNames.remove("Cversion");
        columns.removeIf(f -> f.name.equals("Cversion"));

        boolean identity = ids
                .stream()
                .anyMatch(f -> f.identity && !f.isFromParent());

        String updateWhereVersion = "";
        if(versionField != null) {
            String versionColumnName = versionField.name.substring(1, versionField.name.length()); //Remove the prefix by doing the subString
            updateWhereVersion = String.format(" and %s = ?", versionColumnName);
        }

        insertQuery = (identity ? columnsNames.stream() : Stream.concat(idName.stream(), columnsNames.stream()))
                .collect(Collectors.joining(", ", "insert into " + type.getSimpleName() + " ( ", " ) "))
                //+ "output " + idsNames + "CAST(INSERTED.version as bigint) version "
                + (identity ? columnsNames.stream() : Stream.concat(idName.stream(), columnsNames.stream()))
                .map(c -> "?")
                .collect(Collectors.joining(", ", "values ( ", " )"));

        updateQuery = columnsNames
                .stream()
                .map(c -> c + " = ?")
                .collect(Collectors.joining(", ", "update " + type.getSimpleName() + " set ", " where ")) //output CAST(INSERTED.version as bigint) version
                + idName
                .stream()
                .map(id -> id + " = ?")
                .collect(Collectors.joining(" and "))
                + updateWhereVersion;

        deleteQuery = idName
                .stream()
                .map(id -> id + " = ?")
                .collect(Collectors.joining(" and ", "delete from " + type.getSimpleName() + " where ", ""));
    }

    static class FieldOperations {
        public final Predicate<Field> predicate;
        public final BiFunction<Field, String, Stream<SqlField>> function;

        public FieldOperations(Predicate<Field> predicate, BiFunction<Field, String, Stream<SqlField>> function) {
            this.predicate = predicate;
            this.function = function;
        }
    }

    private final FieldOperations[] operations;

    private FieldOperations[] initOperations(Class<?> type) {
        return new FieldOperations[]{
                new FieldOperations(f -> f.isAnnotationPresent(EmbeddedId.class), (f, pref) -> {
                    primaryKeyType = f.getType();
                    if (!EmbeddedIdClass.class.isAssignableFrom(primaryKeyType))
                        throw new DataMapperException("The field " + f.getName() + " on " + type.getSimpleName() + " annotated with @EmbeddedId should extend EmbeddedIdClass!");

                    try {
                        primaryKeyConstructor = primaryKeyType.getConstructor();
                    } catch (NoSuchMethodException e) {
                        throw new DataMapperException(e);
                    }

                    return Arrays.stream(f.getType().getDeclaredFields())
                            .filter(fieldPredicate)
                            .map(fi -> new SqlFieldId(fi, fi.getName(), pref + fi.getName(), false, true));
                }),

                new FieldOperations(f -> f.isAnnotationPresent(Id.class),
                        (f, pref) -> Stream.of(new SqlFieldId(f, f.getName(), pref + f.getName(), f.getAnnotation(Id.class).isIdentity(), false))),

                new FieldOperations(f -> f.isAnnotationPresent(ColumnName.class),
                        (f, pref) -> Stream.of(new SqlFieldExternal(f, pref))),

                new FieldOperations(f -> f.isAnnotationPresent(Version.class),
                        (f, pref) -> Stream.of(new SqlFieldVersion(
                                f,
                                pref.substring(0, pref.length() - 1) + f.getName(),
                                String.format("CAST(%s%s as bigint) %s%s", pref, f.getName(), pref.substring(0, pref.length() - 1), f.getName())
                        ))),

                new FieldOperations(fieldPredicate,
                        (f, pref) -> Stream.of(new SqlField(f, f.getName(), pref + f.getName())))
        };
    }

    private Stream<SqlField> toSqlField(Field f, String queryPrefix) {
        for (FieldOperations op : operations) {
            if (op.predicate.test(f))
                return op.function.apply(f, queryPrefix);
        }
        return Stream.empty();
    }

    public String getSelectQuery() {
        return selectQuery;
    }

    public String getSelectCountQuery() {
        return selectCountQuery;
    }

    public String getInsertQuery() {
        return insertQuery;
    }

    public String getUpdateQuery() {
        return updateQuery;
    }

    public String getDeleteQuery() {
        return deleteQuery;
    }

    public String getSelectByIdQuery() {
        return selectByIdQuery;
    }

    public String getPagination() { return pagination; }

    public List<SqlFieldId> getIds() {
        return ids;
    }

    public List<SqlField> getColumns() {
        return columns;
    }

    public List<SqlFieldExternal> getExternals() {
        return externals;
    }

    public List<SqlField> getAllFields() {
        return allFields;
    }

    public Predicate<Field> getFieldPredicate() {
        return fieldPredicate;
    }

    public Class<?> getPrimaryKeyType() {
        return primaryKeyType;
    }

    public Constructor getPrimaryKeyConstructor() {
        return primaryKeyConstructor;
    }

    public SqlFieldVersion getVersionField() {
        return versionField;
    }

    public Constructor<?> getConstructor() {
        return constructor;
    }
}
