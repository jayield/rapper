package org.github.isel.rapper.utils;

import com.sun.java.swing.plaf.windows.WindowsTreeUI;
import org.github.isel.rapper.EmbeddedId;
import org.github.isel.rapper.Id;

import java.lang.reflect.Field;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MapperSettings {

    private final Class<?> type;
    private final List<Field> fields;
    private List<Field> id = new ArrayList<>();
    private List<String> idName = new ArrayList<>();
    private boolean identity;
    private List<Field> columns = new ArrayList<>();
    private List<String> columnsNames = new ArrayList<>();
    private String selectQuery;
    private String insertQuery;
    private String updateQuery;
    private String deleteQuery;
    private String selectByIdQuery;

    public MapperSettings(Class<?> type){
        this.type = type;
        this.fields = Arrays.stream(type.getDeclaredFields())
                .filter(f ->
                        f.isAnnotationPresent(EmbeddedId.class) ||
                                f.getType().isPrimitive() ||
                                f.getType().isAssignableFrom(String.class) ||
                                f.getType().isAssignableFrom(Date.class))
                .collect(Collectors.toList());
//        fields.addAll(MapperRegistry.getMapper(type.getSuperclass()).map(m -> m.getMapperSettings().getFields()).orElse(Collections.emptyList()));
        buildQueryStrings();
    }

    private void buildQueryStrings(){
        fieldSeparator(fields);

        selectQuery = Stream.concat(idName.stream(), columnsNames.stream()).collect(Collectors.joining(", ", "select ", " from "+type.getSimpleName()));
        selectByIdQuery = selectQuery + idName.stream().map(id -> id+" = ?").collect(Collectors.joining(" and ", " where ", ""));
        insertQuery = (identity ? columnsNames.stream() : Stream.concat(idName.stream(), columnsNames.stream()))
                        .collect(Collectors.joining(", ","insert into "+type.getSimpleName()+" ( ", " ) ")) +
                    (identity ? "output inserted."+idName.get(0)+" " : "") +
                    (identity ? columnsNames.stream() : Stream.concat(idName.stream(), columnsNames.stream()))
                        .map(c->"?").collect(Collectors.joining(", ", "values ( ", " )"));

        updateQuery = columnsNames.stream().map(c -> c + " = ?").collect(Collectors.joining(", ","update "+type.getSimpleName()+" set "," where ")) +
                idName.stream().map(id->id+" = ?").collect(Collectors.joining(" and "));
        deleteQuery = idName.stream().map(id -> id + " = ?").collect(Collectors.joining(" and ", "delete from "+type.getSimpleName()+" where ",""));
    }

    private void fieldSeparator(List<Field> fields){
        fields.forEach(f -> {
            if (f.isAnnotationPresent(EmbeddedId.class)){ //chaves compostas
                Arrays.stream(f.getType().getDeclaredFields()).filter(field ->
                        field.getType().isPrimitive() ||
                                field.getType().isAssignableFrom(String.class) ||
                                field.getType().isAssignableFrom(Date.class)).peek(id::add).map(Field::getName).forEach(idName::add);
            } else if(f.isAnnotationPresent(Id.class)){
                if(f.getAnnotation(Id.class).isIdentity()){
                    identity = true;
                }
                idName.add(f.getName());
                id.add(f);
            } else{
                columnsNames.add(f.getName());
                columns.add(f);
            }
        });
    }

    public Class<?> getType() {
        return type;
    }

    public List<Field> getFields() {
        return fields;
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

    public List<String> getIdName() {
        return idName;
    }

    public boolean isIdentity() {
        return identity;
    }

    public List<String> getColumnsNames() {
        return columnsNames;
    }

    public List<Field> getId() {
        return id;
    }

    public List<Field> getColumns() {
        return columns;
    }
}
