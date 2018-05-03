package com.github.jayield.rapper.utils;


import com.github.jayield.rapper.ColumnName;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.EmbeddedId;
import com.github.jayield.rapper.Id;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.jayield.rapper.utils.SqlField.*;

public class MapperSettings {

    private final Class<?> type;

    private List<SqlField> columns = new ArrayList<>();
    private List<SqlFieldId> ids = new ArrayList<>();
    private List<SqlFieldExternal> externals = new ArrayList<>();
    private List<SqlField> allFields = new ArrayList<>();

    private String selectQuery;
    private String insertQuery;
    private String updateQuery;
    private String deleteQuery;
    private String selectByIdQuery;

    private final Predicate<Field> fieldPredicate = field ->
            field.getType().isPrimitive()
            || field.getType().isAssignableFrom(String.class)
            || field.getType().isAssignableFrom(Timestamp.class)
            || field.getType().isAssignableFrom(Date.class)
            || field.getType().isAssignableFrom(CompletableFuture.class);

    public MapperSettings(Class<?> type) {
        this.type = type;

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

        Optional.ofNullable(fieldMap.get(SqlField.class)).ifPresent(sqlFields -> columns.addAll(sqlFields));

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
                    .peek(sqlFieldId -> sqlFieldId.setFromParent())
                    .collect(Collectors.toList());

            allFields.addAll(parentFieldMap.get(SqlField.class));
            ids.addAll(sqlFieldIds);
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
        suffix.append(" from [").append(type.getSimpleName()).append("] C ");

        int[] i = {1};
        for (Class<?> clazz = type.getSuperclass(); clazz != Object.class && DomainObject.class.isAssignableFrom(clazz); clazz = clazz.getSuperclass(), i[0]++) {
            suffix.append("inner join [").append(clazz.getSimpleName()).append(String.format("] P%d ", i[0])).append("on ");

            //Set the comparisions
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

        selectByIdQuery = selectQuery +
                idName.stream()
                        .map(id -> id + " = ?")
                        .collect(Collectors.joining(" and ", " where ", ""));

        idName = ids
                .stream()
                .map(f -> f.name)
                .collect(Collectors.toList());

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

        //TODO clean
        String collect = ids
                .stream()
                .filter(f -> f.identity && !f.isFromParent())
                .map(f -> "INSERTED." + f.name)
                .collect(Collectors.joining(", ", "", ", "));
        String idsNames = "";
        if (!collect.equals(", ")) idsNames = collect;

        //columnsNames.addAll(externals.stream().flatMap(sqlFieldExternal -> Arrays.stream(sqlFieldExternal.names)).collect(Collectors.toList()));

        insertQuery = (identity ? columnsNames.stream() : Stream.concat(idName.stream(), columnsNames.stream()))
                .collect(Collectors.joining(", ", "insert into [" + type.getSimpleName() + "] ( ", " ) "))
                + "output " + idsNames + "CAST(INSERTED.version as bigint) version "
                + (identity ? columnsNames.stream() : Stream.concat(idName.stream(), columnsNames.stream()))
                .map(c -> "?")
                .collect(Collectors.joining(", ", "values ( ", " )"));

        updateQuery = columnsNames
                .stream()
                .map(c -> c + " = ?")
                .collect(Collectors.joining(", ", "update [" + type.getSimpleName() + "] set ", " output CAST(INSERTED.version as bigint) version where "))
                + idName
                .stream()
                .map(id -> id + " = ?")
                .collect(Collectors.joining(" and "))
                + " and version = ?";

        deleteQuery = idName
                .stream()
                .map(id -> id + " = ?")
                .collect(Collectors.joining(" and ", "delete from [" + type.getSimpleName() + "] where ", ""));
    }

    class FieldOperations {
        public final Predicate<Field> predicate;
        public final BiFunction<Field, String, Stream<SqlField>> function;

        public FieldOperations(Predicate<Field> predicate, BiFunction<Field, String, Stream<SqlField>> function) {
            this.predicate = predicate;
            this.function = function;
        }
    }

    private final FieldOperations[] operations = {
            new FieldOperations(f -> f.isAnnotationPresent(EmbeddedId.class), (f, pref) -> Arrays.stream(f.getType().getDeclaredFields())
                    .filter(fieldPredicate)
                    .map(fi -> new SqlFieldId(fi, getName(fi, pref), getQueryValue(fi, pref), false, true))),

            new FieldOperations(f -> f.isAnnotationPresent(Id.class),
                    (f, pref) -> Stream.of(new SqlFieldId(f, getName(f, pref), getQueryValue(f, pref), f.getAnnotation(Id.class).isIdentity(), false))),

            new FieldOperations(f -> f.isAnnotationPresent(ColumnName.class),
                    (f, pref) -> Stream.of(new SqlFieldExternal(f, pref))),

            new FieldOperations(fieldPredicate,
                    (f, pref) -> Stream.of(new SqlField(f, getName(f, pref), getQueryValue(f, pref))))
    };

    private String getName(Field f, String pref) {
        if (f.getName().equals("version")) return pref.substring(0, pref.length() - 1) + f.getName();
        else return f.getName();
    }

    private String getQueryValue(Field f, String pref) {
        if (f.getName().equals("version"))
            return String.format("CAST(%sversion as bigint) %sversion", pref, pref.substring(0, pref.length() - 1));
        else return pref + f.getName();
    }

    private Stream<SqlField> toSqlField(Field f, String queryPrefix) {
        for (FieldOperations op : operations) {
            if (op.predicate.test(f))
                return op.function.apply(f, queryPrefix);
        }
        return Stream.empty();
    }

    public Class<?> getType() {
        return type;
    }

    public String getSelectQuery() {
        return selectQuery;
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
}
