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
        public final String[] foreignNames;
        public final Class<? extends DomainObject> type;
        public final Class<?> fType;
        //public final String columnName;
        public final String table;
        public final String selectTableQuery;
        public final String[] columnsNames;

        public SqlFieldExternal(Field field, Class<?> fType, String name, String columnName, String table, String foreignName, Class<? extends DomainObject> type) {
            super(field, name);
            this.fType = fType;
            //this.columnName = columnName;
            this.table = table;
            this.foreignNames = foreignName.split("\\|");
            this.type = type;
            this.columnsNames = columnName.split("\\|");

            StringBuilder sb = new StringBuilder();
            sb.append("select * from ").append(table).append(" where ");
            for (int i = 0; i < columnsNames.length; i++) {
                sb.append(columnsNames[i]).append(" = ? ");
                if(i + 1 != columnsNames.length) sb.append("and ");
            }

            selectTableQuery = sb.toString();
        }
    }
}


