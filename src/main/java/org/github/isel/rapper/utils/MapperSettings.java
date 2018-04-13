package org.github.isel.rapper.utils;


import org.github.isel.rapper.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.github.isel.rapper.utils.SqlField.*;

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

    private final Predicate<Field> fieldPredicate = field -> field.getType().isPrimitive() ||
            field.getType().isAssignableFrom(String.class) ||
            field.getType().isAssignableFrom(Timestamp.class) ||
            field.getType().isAssignableFrom(Date.class);

    public MapperSettings(Class<?> type){
        this.type = type;

        Map<Class, List<SqlField>> fieldMap = Arrays.stream(type.getDeclaredFields())
                .flatMap(this::toSqlField)
                .collect(Collectors.groupingBy(SqlField::getClass));

        getParentsFields(type);

        Optional.ofNullable(fieldMap.get(SqlFieldId.class))
                .ifPresent(sqlFields ->
                    ids.addAll(sqlFields.stream().map(f -> ((SqlFieldId) f)).collect(Collectors.toList()))
                );

        Optional.ofNullable(fieldMap.get(SqlField.class))
                .ifPresent(columns::addAll);

        Optional.ofNullable(fieldMap.get(SqlFieldExternal.class))
                .ifPresent(sqlFields -> externals = sqlFields.stream().map(f -> (SqlFieldExternal) f).collect(Collectors.toList()));

        allFields.addAll(ids);
        allFields.addAll(columns);
        allFields.addAll(externals);

        buildQueryStrings();
    }

    /**
     * Add ids and fields from parent classes to ids and allFields respectively
     * @param type
     */
    private void getParentsFields(Class<?> type) {
        for(Class<?> clazz = type.getSuperclass(); clazz != Object.class && DomainObject.class.isAssignableFrom(clazz); clazz = clazz.getSuperclass()){
            Map<Class, List<SqlField>> parentFieldMap = Arrays.stream(clazz.getDeclaredFields())
                    .flatMap(this::toSqlField)
                    .collect(Collectors.groupingBy(SqlField::getClass));

            List<SqlField> sqlFieldIds = parentFieldMap.getOrDefault(SqlFieldId.class, new ArrayList<>());
            allFields.addAll(parentFieldMap.get(SqlField.class));
            ids.addAll(sqlFieldIds.stream().map(f -> ((SqlFieldId) f)).collect(Collectors.toList()));
        }
    }

    private void buildQueryStrings(){
        List<String> idName = ids
                .stream()
                .map(f->f.name)
                .collect(Collectors.toList());

        List<String> columnsNames = columns
                .stream()
                .map(f->f.name)
                .collect(Collectors.toList());

        selectQuery = Stream.concat(idName.stream(), columnsNames.stream())
                .map(str -> {
                    if(str.equals("version")) return "CAST(version as bigint) version";
                    return str;
                })
                .collect(Collectors.joining(", ", "select ", " from "+type.getSimpleName()));

        selectByIdQuery = selectQuery +
                idName.stream()
                        .map(id -> id+" = ?")
                        .collect(Collectors.joining(" and ", " where ", ""));

        columnsNames.remove("version");
        columns.removeIf(f-> f.name.equals("version"));

        boolean identity = ids
                .stream()
                .anyMatch(f -> f.identity);

        insertQuery = (identity ? columnsNames.stream() : Stream.concat(idName.stream(), columnsNames.stream()))
                .collect(Collectors.joining(", ","insert into "+type.getSimpleName()+" ( ", " ) ")) +
                "output CAST(INSERTED.version as bigint) version " +
                (identity ? columnsNames.stream() : Stream.concat(idName.stream(), columnsNames.stream()))
                        .map(c -> "?")
                        .collect(Collectors.joining(", ", "values ( ", " )"));

        updateQuery = columnsNames
                .stream()
                .map(c -> c + " = ?")
                .collect(Collectors.joining(", ","update " + type.getSimpleName() + " set "," output CAST(INSERTED.version as bigint) version where "))
                + idName.stream()
                .map(id -> id + " = ?")
                .collect(Collectors.joining(" and "))
                + " and version = ?";

        deleteQuery = idName.stream()
                .map(id -> id + " = ?")
                .collect(Collectors.joining(" and ", "delete from " + type.getSimpleName() + " where ",""));
    }

    class FieldOperations {
        public final Predicate<Field> predicate;
        public final Function<Field, Stream<SqlField>> function;

        public FieldOperations(Predicate<Field> predicate, Function<Field, Stream<SqlField>> function) {
            this.predicate = predicate;
            this.function = function;
        }
    }

    private final FieldOperations[] operations = {
            new FieldOperations(f->f.isAnnotationPresent(EmbeddedId.class), f-> Arrays.stream(f.getType().getDeclaredFields())
                    .filter(fieldPredicate)
                    .map(fi-> new SqlFieldId(fi, fi.getName(), false, true))),
            new FieldOperations(f->f.isAnnotationPresent(Id.class), f->Stream.of(new SqlFieldId(f, f.getName(), f.getAnnotation(Id.class).isIdentity(), false))),
            new FieldOperations(f->f.isAnnotationPresent(ColumnName.class), f->Stream.of(new SqlFieldExternal(
                            f,
                            f.getType(),
                            f.getName(),
                            f.getAnnotation(ColumnName.class).name(),
                            f.getAnnotation(ColumnName.class).table(),
                            f.getAnnotation(ColumnName.class).foreignName(),
                            ReflectionUtils.getGenericType(f.getGenericType())
                    )
            )),
            new FieldOperations(fieldPredicate, f->Stream.of(new SqlField(f, f.getName())))
    };

    private Stream<SqlField> toSqlField(Field f){
        for(FieldOperations op : operations){
            if(op.predicate.test(f))
                return op.function.apply(f);
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
