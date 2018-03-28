package org.github.isel.rapper.utils;

import org.github.isel.rapper.DomainObject;

import java.lang.reflect.Field;

public class SqlField {
    public final Field field;
    public final String name;

    public SqlField(Field field, String name) {
        this.field = field;
        this.name = name;
    }

    public static class SqlFieldId extends SqlField{
        public final boolean identity;

        public SqlFieldId(Field field, String name, boolean identity) {
            super(field, name);
            this.identity = identity;
        }
    }

    public static class SqlFieldExternal extends SqlField{
        public final String columnName;
        public final Class<? extends DomainObject> type;

        public SqlFieldExternal(Field field, String name, String columnName, Class<? extends DomainObject> type) {
            super(field, name);
            this.columnName = columnName;
            this.type = type;
        }
    }
}


