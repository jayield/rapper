package com.github.jayield.rapper.sql;

import java.lang.reflect.Field;

public class SqlFieldVersion extends SqlField {

    public SqlFieldVersion(Field field, String name, String selectQueryValue) {
        super(field, name, selectQueryValue);
    }
}