package com.github.jayield.rapper.sql;

import com.github.jayield.rapper.exceptions.DataMapperException;

import java.lang.reflect.Field;
import java.util.stream.Stream;

public class SqlField {
    public static final String SQL_FIELD_EXTERNAL = "SqlFieldExternal";

    protected final Field field;
    protected final String name;
    private final String selectQueryValue;

    public SqlField(Field field, String name, String selectQueryValue) {
        this.field = field;
        this.name = name;
        this.selectQueryValue = selectQueryValue;
    }

    public Stream<Object> getValuesForStatement(Object obj) {
        field.setAccessible(true);
        try {
            return Stream.of( obj != null ? field.get(obj) : null);
        } catch (IllegalAccessException | IllegalStateException e) {
            throw new DataMapperException(e);
        }
    }

    public int byUpdate(){
        return 0;
    }

    public int byInsert(){
        return 2;
    }

    public Field getField() {
        return field;
    }

    public String getName() {
        return name;
    }

    public String getSelectQueryValue() {
        return selectQueryValue;
    }
}
