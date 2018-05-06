package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.ColumnName;
import com.github.jayield.rapper.DataMapper;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.exceptions.DataMapperException;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SqlField {
    public final Field field;
    public final String name;
    public final String selectQueryValue;
    public static final String SQL_FIELD_EXTERNAL = "SqlFieldExternal";

    public SqlField(Field field, String name, String selectQueryValue) {
        this.field = field;
        this.name = name;
        this.selectQueryValue = selectQueryValue;
    }

    public<T> int setValueInStatement(PreparedStatement stmt, int index, T obj) {
        field.setAccessible(true);
        try {
            stmt.setObject(index, obj != null ? field.get(obj) : null);
        } catch (SQLException | IllegalAccessException e) {
            throw new DataMapperException(e);
        }
        return 0;
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

    public static class SqlFieldId extends SqlField{
        public final boolean identity;
        public final boolean embeddedId;
        private boolean isFromParent = false;

        public SqlFieldId(Field field, String name, String queryValue, boolean identity, boolean embeddedId) {
            super(field, name, queryValue);
            this.identity = identity;
            this.embeddedId = embeddedId;
        }

        @Override
        public <T> int setValueInStatement(PreparedStatement stmt, int index, T obj) {
            Object key = null;
            if(obj != null) {
                if (DomainObject.class.isAssignableFrom(obj.getClass()))
                    key = ((DomainObject) obj).getIdentityKey();
                else
                    key = obj;
            }

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
            return 0;
        }

        @Override
        public int byUpdate() {
            return 1;
        }

        @Override
        public int byInsert() {
            return identity && !isFromParent ? 3 : 0;
        }

        public boolean isFromParent() {
            return isFromParent;
        }

        void setFromParent() {
            isFromParent = true;
        }
    }

    public static class SqlFieldExternal extends SqlField{
        public final Class<?> type;                                         //Type of the field that holds the collection
        public final Class<? extends DomainObject> domainObjectType;        //Type of the elements in the collection
        public final String[] names;
        public final String[] foreignNames;
        public final String[] externalNames;
        public final String selectTableQuery;
        private final List<Integer> values;
        private Object[] idValues;

        //TODO if name defined check if type == CompletableFuture<DomainObject>, else check type == CompletableFuture<List<DomainObject>>
        public SqlFieldExternal(Field field, String pref) {
            super(field, SQL_FIELD_EXTERNAL, buildSelectQueryValue(field, pref));
            type = field.getType();
            domainObjectType = ReflectionUtils.getGenericType(field.getGenericType());
            ColumnName annotation = field.getAnnotation(ColumnName.class);
            names = annotation.name();
            foreignNames = annotation.foreignName();
            externalNames = annotation.externalName();
            selectTableQuery = buildSelectQuery(annotation.table());
            values = getBytes(annotation);
        }

        private static String buildSelectQueryValue(Field field, String pref) {
            ColumnName annotation = field.getAnnotation(ColumnName.class);
            String[] names = annotation.name();
            StringBuilder sb = new StringBuilder();
            if (names.length != 0)
                for (int i = 0; i < names.length; i++) {
                    sb.append(pref).append(names[i]);
                    if (i + 1 != names.length) sb.append(", ");
                }
            return sb.toString();
        }

        /**
         * Used for ExternalsHandler mapper to know what to execute, depending on ColumnName values
         * @param annotation ColumnName annotation
         * @return
         */
        private ArrayList<Integer> getBytes(ColumnName annotation) {
            try {
                String[] nameDefaultValue = (String[]) ColumnName.class.getDeclaredMethod("name").getDefaultValue();
                String[] foreignNameDefaultValue = (String[]) ColumnName.class.getDeclaredMethod("foreignName").getDefaultValue();
                String tableDefaultValue = (String) ColumnName.class.getDeclaredMethod("table").getDefaultValue();

                boolean nameEqualsDefaultValue = Arrays.equals(annotation.name(), nameDefaultValue);
                boolean foreignNameEqualsDefaultValue = Arrays.equals(annotation.foreignName(), foreignNameDefaultValue);
                if (!nameEqualsDefaultValue && !foreignNameEqualsDefaultValue)
                    throw new DataMapperException("The annotation ColumnName shouldn't have both name and foreignName defined!");

                ArrayList<Integer> bytes = new ArrayList<>(4);
                bytes.add(Arrays.equals(annotation.name(), nameDefaultValue) ? 0 : 1);
                bytes.add(Arrays.equals(annotation.foreignName(), foreignNameDefaultValue) ? 0 : 1);
                bytes.add(annotation.table().equals(tableDefaultValue) ? 0 : 1);
                bytes.add(Arrays.equals(annotation.externalName(), nameDefaultValue) ? 0 : 1);
                return bytes;
            } catch (NoSuchMethodException e) {
                throw new DataMapperException(e);
            }
        }

        private String buildSelectQuery(String table) {
            StringBuilder sb = new StringBuilder();
            sb.append("select * from ").append(table).append(" where ");
            for (int i = 0; i < foreignNames.length; i++) {
                sb.append(foreignNames[i]).append(" = ? ");
                if(i + 1 != foreignNames.length) sb.append("and ");
            }
            return sb.toString();
        }

        @Override
        public <T> int setValueInStatement(PreparedStatement stmt, int index, T obj) {
            try {
                field.setAccessible(true);
                CompletableFuture<? extends DomainObject> cp = (CompletableFuture<? extends DomainObject>) field.get(obj);
                //TODO remove join
                //It will get the value from the completableFuture, get the value's SqlFieldIds and call its setValueInStatement(), incrementing the index.
                DomainObject domainObject = cp != null ? cp.join() : null;
                int[] i = {index};
                ((DataMapper) MapperRegistry.getRepository(domainObjectType).getMapper())
                        .getMapperSettings()
                        .getIds()
                        .forEach(sqlFieldId -> {
                            sqlFieldId.setValueInStatement(stmt, i[0], domainObject);
                            i[0]++;
                        });
                return i[0] - index - 1;
            } catch (IllegalAccessException e) {
                throw new DataMapperException(e);
            }
        }

        public List<Integer> getValues() {
            return values;
        }

        public Object[] getIdValues() {
            return idValues;
        }

        public void setIdValues(Object[] idValues) {
            this.idValues = idValues;
        }
    }
}


