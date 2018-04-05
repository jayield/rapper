package org.github.isel.rapper.utils;


import org.github.isel.rapper.ColumnName;
import org.github.isel.rapper.EmbeddedId;
import org.github.isel.rapper.Id;

import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.github.isel.rapper.utils.SqlField.*;

public class MapperSettings {

    private final Class<?> type;
//    private List<Field> fields = new ArrayList<>();
//    private List<Field> externals = new ArrayList<>();
//    private List<Field> allFields = new ArrayList<>();
//    private List<Field> id = new ArrayList<>();
//    private List<String> idName = new ArrayList<>();
//    private List<Field> columns = new ArrayList<>();
//    private List<String> columnsNames = new ArrayList<>();

    private List<SqlField> columns;
    private List<SqlFieldId> ids;
    private List<SqlFieldExternal> externals;
    private List<SqlField> allFields;

    private String selectQuery;
    private String insertQuery;
    private String updateQuery;
    private String deleteQuery;
    private String selectByIdQuery;

    public MapperSettings(Class<?> type){
        this.type = type;

        Map<Class, List<SqlField>> fieldMap = (Arrays.stream(type.getDeclaredFields())
                .flatMap(this::toSqlField)
                .collect(Collectors.groupingBy(SqlField::getClass)));

        //Add SqlFieldIds which have "identity = true" to columns
        Optional.ofNullable(fieldMap.get(SqlFieldId.class))
                .ifPresent(sqlFields -> {
                    columns = new ArrayList<>();
                    sqlFields
                            .stream()
                            .map(sqlField -> ((SqlFieldId) sqlField))
                            .filter(sqlFieldId -> sqlFieldId.identity)
                            .forEach(columns::add);
                });

        Optional.ofNullable(fieldMap.get(SqlField.class))
                .ifPresent(sqlFields -> {
                    if(columns == null) columns = new ArrayList<>();
                    columns.addAll(sqlFields);
                });

        Optional.ofNullable(fieldMap.get(SqlFieldId.class))
                .ifPresent(sqlFields -> ids = sqlFields.stream().map(f -> ((SqlFieldId) f)).collect(Collectors.toList()));
        Optional.ofNullable(fieldMap.get(SqlFieldExternal.class))
                .ifPresent(sqlFields -> externals = sqlFields.stream().map(f -> (SqlFieldExternal) f).collect(Collectors.toList()));

        allFields = new ArrayList<>();
        if(ids != null) allFields.addAll(ids);
        if(columns != null) columns.forEach(sqlField -> { if(!allFields.contains(sqlField)) allFields.add(sqlField); });
        if(externals != null) allFields.addAll(externals);

        buildQueryStrings();
    }

    private void buildQueryStrings(){
        List<String> idName = ids.stream().map(f->f.name).collect(Collectors.toList());
        List<String> columnsNames = columns.stream().map(f->f.name).collect(Collectors.toList());

        selectQuery = Stream
                .concat(idName.stream(), columnsNames.stream())
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

        //boolean identity = ids.stream().allMatch(f -> f.identity);

        insertQuery = columnsNames
                .stream()
                .collect(Collectors.joining(", ","insert into "+type.getSimpleName()+" ( ", " ) ")) +
                "output CAST(INSERTED.version as bigint) version " +
                columnsNames
                        .stream()
                        .map(c->"?")
                        .collect(Collectors.joining(", ", "values ( ", " )"));

        updateQuery = columnsNames.stream()
                .map(c -> c + " = ?")
                .collect(Collectors.joining(", ","update "+type.getSimpleName()+" set "," output CAST(INSERTED.version as bigint) version where "))
                + idName.stream()
                .map(id->id+" = ?")
                .collect(Collectors.joining(" and "));

        deleteQuery = idName.stream()
                .map(id -> id + " = ?")
                .collect(Collectors.joining(" and ", "delete from "+type.getSimpleName()+" where ",""));
    }

    private Stream<SqlField> toSqlField(Field f){
        Predicate<Field> pred = field -> field.getType().isPrimitive() ||
                field.getType().isAssignableFrom(String.class) ||
                field.getType().isAssignableFrom(Timestamp.class);
        if(f.isAnnotationPresent(EmbeddedId.class)){
            return Arrays.stream(f.getType()
                    .getDeclaredFields()).filter(pred)
                    .map(fi-> new SqlFieldId(fi, fi.getName(), false));
        }
        if(f.isAnnotationPresent(Id.class)){
            return Stream.of(new SqlFieldId(f, f.getName(), f.getAnnotation(Id.class).isIdentity()));
        }
        if(f.isAnnotationPresent(ColumnName.class)){
            return Stream.of(new SqlFieldExternal(
                    f,
                    f.getName(),
                    f.getAnnotation(ColumnName.class).name(),
                    ReflectionUtils.getGenericType(f.getGenericType()))
            );
        }
        if(pred.test(f)){
            return Stream.of(new SqlField(f, f.getName()));
        }
        return Stream.empty();
    }

//    private void fieldSeparator(List<Field> fields){
//        fields.forEach(f -> {
//            if (f.isAnnotationPresent(EmbeddedId.class)){ //chaves compostas
//                Arrays.stream(f.getType().getDeclaredFields()).filter(field ->
//                        field.getType().isPrimitive() ||
//                                field.getType().isAssignableFrom(String.class) ||
//                                field.getType().isAssignableFrom(Date.class)).peek(id::add).map(Field::getName).forEach(idName::add);
//            } else if(f.isAnnotationPresent(Id.class)){
//                if(f.getAnnotation(Id.class).isIdentity()){
//                    identity = true;
//                }
//                idName.add(f.getName());
//                id.add(f);
//            } else{
//                columnsNames.add(f.getName());
//                columns.add(f);
//            }
//        });
//    }

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

//    public List<String> getIdName() {
//        return idName;
//    }

//    public boolean isIdentity() {
//        return identity;
//    }

//    public List<String> getColumnsNames() {
//        return columnsNames;
//    }

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
}
