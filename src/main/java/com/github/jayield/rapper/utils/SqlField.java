package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.ColumnName;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.exceptions.DataMapperException;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        public Stream<Object> getValuesForStatement(Object obj) {
            Object key = null;
            if(obj != null) {
                if (DomainObject.class.isAssignableFrom(obj.getClass()))
                    key = ((DomainObject) obj).getIdentityKey();
                else
                    key = obj;
            }
            if(embeddedId)
                return super.getValuesForStatement(key);
            else
                return Stream.of(key);
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

    public static class SqlFieldVersion extends SqlField {

        public SqlFieldVersion(Field field, String name, String selectQueryValue) {
            super(field, name, selectQueryValue);
        }
    }

    public static class SqlFieldExternal extends SqlField{
        public final Class<?> type;                                         //Type of the field that holds the collection
        public final Class<? extends DomainObject> domainObjectType;        //Type of the elements in the collection
        public final String[] names;
        public final String[] foreignNames;
        public final String[] externalNames;
        public final String selectTableQuery;
        public final Object mon = new Object();
        private final Class<? extends Populate> populateStrategy;
        private Object[] foreignKey;

        public SqlFieldExternal(Field field, String pref) {
            super(field, SQL_FIELD_EXTERNAL, buildSelectQueryValue(field, pref));
            type = field.getType();

            ColumnName annotation = field.getAnnotation(ColumnName.class);
            names = annotation.name();
            foreignNames = annotation.foreignName();
            externalNames = annotation.externalName();

            verifyField();

            if(type == Function.class)
                domainObjectType = ReflectionUtils.getGenericType(((ParameterizedType) field.getGenericType()).getActualTypeArguments()[1]); //Index number 1 is the return of the Function
            else
                domainObjectType = (Class<? extends DomainObject>) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
            selectTableQuery = buildSelectQuery(annotation.table());
            populateStrategy = getStrategy(annotation);
        }

        /**
         * It will verify if the field follows the rules
         */
        private void verifyField() {
            try {
                if (type != Function.class && type != Foreign.class)
                    throw new DataMapperException("The field annotated with @ColumnName must be of type Function or Foreign");
                String[] nameDefaultValue = (String[]) ColumnName.class.getDeclaredMethod("name").getDefaultValue();

                checkNameParameter(nameDefaultValue);
            } catch (NoSuchMethodException e) {
                throw new DataMapperException(e);
            }
        }

        private void checkNameParameter(String[] nameDefaultValue) {
            if(!Arrays.equals(this.names, nameDefaultValue)) {
                if (type != Foreign.class)
                    throw new DataMapperException("The field annotated with @ColumnName with \"name\" defined must be of type Foreign");
            }
            else {
                Type[] typeArguments = ((ParameterizedType) field.getGenericType()).getActualTypeArguments();

                if(typeArguments[0] != UnitOfWork.class)
                    throw new DataMapperException("The field annotated with @ColumnName must be a Function<UnitOfWork, CompletableFuture>");

                Type genericType = typeArguments[1];
                if(((ParameterizedType) genericType).getRawType() != CompletableFuture.class)
                    throw new DataMapperException("The field annotated with @ColumnName must be a Function<UnitOfWork, CompletableFuture>");

                genericType = ((ParameterizedType) genericType).getActualTypeArguments()[0];

                if(((ParameterizedType) genericType).getRawType() != List.class)
                    throw new DataMapperException("The field annotated with @ColumnName without \"name\" defined must be a Function<UnitOfWork, CompletableFuture<List<DomainObject>>>");
            }
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
        private Class<? extends Populate> getStrategy(ColumnName annotation) {
            try {
                String[] nameDefaultValue = (String[]) ColumnName.class.getDeclaredMethod("name").getDefaultValue();
                String[] foreignNameDefaultValue = (String[]) ColumnName.class.getDeclaredMethod("foreignName").getDefaultValue();
                String tableDefaultValue = (String) ColumnName.class.getDeclaredMethod("table").getDefaultValue();

                boolean nameEqualsDefaultValue = Arrays.equals(annotation.name(), nameDefaultValue);
                boolean foreignNameEqualsDefaultValue = Arrays.equals(annotation.foreignName(), foreignNameDefaultValue);
                if (!nameEqualsDefaultValue && !foreignNameEqualsDefaultValue)
                    throw new DataMapperException("The annotation ColumnName shouldn't have both name and foreignName defined!");

                boolean p1 = !Arrays.equals(annotation.name(), nameDefaultValue);
                boolean p2 = !Arrays.equals(annotation.foreignName(), foreignNameDefaultValue);
                boolean p3 = p2 && !annotation.table().equals(tableDefaultValue) && !Arrays.equals(annotation.externalName(), nameDefaultValue);

                return Stream.of(new Pair<>(PopulateSingleReference.class, p1), new Pair<>(PopulateWithExternalTable.class, p3), new Pair<>(PopulateMultiReference.class, p2))
                        .filter(Pair::getValue)
                        .findFirst()
                        .map(Pair::getKey)
                        .orElseThrow(() -> new DataMapperException("The annotation is invalid " + annotation));

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

        /**
         * This method will only be called to obtain the primary key of the object when it's a simple relation (ex. CF<DomainObject>)
         * @param obj
         * @return
         */
        @Override
        public Stream<Object> getValuesForStatement(Object obj) {
            try {
                field.setAccessible(true);
                Foreign foreign = (Foreign) field.get(obj);
                Object key = foreign != null ? foreign.getForeignKey() : null;

                List<Stream<Object>> futureList = MapperRegistry.getMapperSettings(domainObjectType)
                        .getIds()
                        .stream()
                        .map(sqlFieldId -> sqlFieldId.getValuesForStatement(key)).collect(Collectors.toList());

                return futureList.stream().flatMap(s -> s);
            } catch (IllegalAccessException e) {
                throw new DataMapperException(e);
            }
        }

        public Object[] getForeignKey() {
            return foreignKey;
        }

        public void setForeignKey(Object[] foreignKey) {
            this.foreignKey = foreignKey;
        }

        public Class<? extends Populate> getPopulateStrategy() {
            return populateStrategy;
        }
    }
}


