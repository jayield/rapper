package org.github.isel.rapper.utils;

import org.github.isel.rapper.DomainObject;
import org.github.isel.rapper.exceptions.DataMapperException;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SqlField {
    public final Field field;
    public final String name;
    public final String selectQueryValue;

    public SqlField(Field field, String name, String selectQueryValue) {
        this.field = field;
        this.name = name;
        this.selectQueryValue = selectQueryValue;
    }

    public<T> void setValueInStatement(PreparedStatement stmt, int index, T obj) {
        field.setAccessible(true);
        try {
            stmt.setObject(index, field.get(obj));
        } catch (SQLException | IllegalAccessException e) {
            throw new DataMapperException(e);
        }
    }

    public int byUpdate(){
        return 0;
    }

    public int byInsert(){
        return 2;
    }

    public static class SqlFieldId extends SqlField{
        public final boolean identity;
        public final boolean embeddedId;
        public boolean isFromParent = false;

        public SqlFieldId(Field field, String name, String queryValue, boolean identity, boolean embeddedId) {
            super(field, name, queryValue);
            this.identity = identity;
            this.embeddedId = embeddedId;
        }

        @Override
        public <T> void setValueInStatement(PreparedStatement stmt, int index, T obj) {
            Object key;
            if(DomainObject.class.isAssignableFrom(obj.getClass()))
                key = ((DomainObject)obj).getIdentityKey();
            else
                key = obj;

            //System.out.println("index " + index + ", key " + key);

            if(embeddedId)
                super.setValueInStatement(stmt, index, key);
            else {
                try {
                    stmt.setObject(index, key);
                } catch (SQLException e) {
                    throw new DataMapperException(e);
                }
            }
        }

        @Override
        public int byUpdate() {
            return 1;
        }

        @Override
        public int byInsert() {
            return identity && !isFromParent ? 3 : 0;
        }
    }

    public static class SqlFieldExternal extends SqlField{
        public final String[] foreignNames;
        //Type of the elements in the collection
        public final Class<? extends DomainObject> type;
        //Type of the collection / supplier
        public final Class<?> fType;
        public final String table;
        public final String selectTableQuery;
        public final String[] columnsNames;

        public SqlFieldExternal(Field field, Class<?> fType, String name, String queryValue, String columnName[], String table, String[] foreignName, Class<? extends DomainObject> type) {
            super(field, name, queryValue);
            this.fType = fType;
            this.table = table;
            this.foreignNames = foreignName;
            this.type = type;
            this.columnsNames = columnName;

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


