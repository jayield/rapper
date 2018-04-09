package org.github.isel.rapper;

import org.github.isel.rapper.utils.ReflectionUtils;
import org.github.isel.rapper.utils.SqlConsumer;
import org.github.isel.rapper.utils.SqlField;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.System.out;
import static org.junit.Assert.*;

public class QueryTests {
    @Test
    public void shouldObtainQueriesForSimpleEntity(){
        DataMapper<Person, Integer> dataMapper = new DataMapper<>(Person.class);

        assertEquals("select nif, name, birthday, CAST(version as bigint) version from Person", dataMapper.getSelectQuery());
        assertEquals("delete from Person where nif = ?", dataMapper.getDeleteQuery());
        assertEquals("insert into Person ( nif, name, birthday ) output CAST(INSERTED.version as bigint) version values ( ?, ?, ? )", dataMapper.getInsertQuery());
        assertEquals("update Person set name = ?, birthday = ? output CAST(INSERTED.version as bigint) version where nif = ?", dataMapper.getUpdateQuery());
    }

    @Test
    public void shouldObtainQueriesForEntitiesWithMultiPK(){
        DataMapper<Car, Car.PrimaryPk> dataMapper = new DataMapper<>(Car.class);

        assertEquals("select owner, plate, brand, model, CAST(version as bigint) version from Car", dataMapper.getSelectQuery());
        assertEquals("delete from Car where owner = ? and plate = ?", dataMapper.getDeleteQuery());
        assertEquals("insert into Car ( owner, plate, brand, model ) output CAST(INSERTED.version as bigint) version values ( ?, ?, ?, ? )", dataMapper.getInsertQuery());
        assertEquals("update Car set brand = ?, model = ? output CAST(INSERTED.version as bigint) version where owner = ? and plate = ?", dataMapper.getUpdateQuery());
    }

    @Test
    public void shouldObtainQueriesForEntitiesWithInheritance(){
        DataMapper<Student, Integer> studentMapper = new DataMapper<>(Student.class);
        DataMapper<TopStudent, Integer> topStudentMapper = new DataMapper<>(TopStudent.class);

        assertEquals("select nif, topGrade, year from TopStudent", topStudentMapper.getSelectQuery());
        assertEquals("delete from TopStudent where nif = ?", topStudentMapper.getDeleteQuery());
        assertEquals("insert into TopStudent ( nif, topGrade, year ) output CAST(INSERTED.version as bigint) version values ( ?, ?, ? )", topStudentMapper.getInsertQuery());
        assertEquals("update TopStudent set topGrade = ?, year = ? output CAST(INSERTED.version as bigint) version where nif = ?", topStudentMapper.getUpdateQuery());

        assertEquals("select nif, studentNumber from Student", studentMapper.getSelectQuery());
        assertEquals("delete from Student where nif = ?", studentMapper.getDeleteQuery());
        assertEquals("insert into Student ( nif, studentNumber ) output CAST(INSERTED.version as bigint) version values ( ?, ? )", studentMapper.getInsertQuery());
        assertEquals("update Student set studentNumber = ? output CAST(INSERTED.version as bigint) version where nif = ?", studentMapper.getUpdateQuery());
    }

    @Test
    public void test() throws NoSuchMethodException {
        Class type = Person.class;

        Field[] fields = type.getDeclaredFields();

        Constructor c = type.getConstructor(Arrays.stream(fields).map(Field::getType).toArray(Class[]::new));

        Arrays.stream(type.getConstructors()[0].getParameters()).map(p->p.getParameterizedType()).forEach(out::println);
    }

    @Test
    public void test2(){
        Class<?> type = B.class;

        List<SqlField> allFields = new ArrayList<>();

        B b = new B();

        for(Class<?> clazz = type; clazz != Object.class; clazz = clazz.getSuperclass()){
            allFields.addAll(Arrays.stream(clazz.getDeclaredFields())
                    .flatMap(this::toSqlField)
                    .collect(Collectors.toList())
            );
        }

        final int[] i = {0};

        SqlConsumer<SqlField> consumer = sqlField -> {
            sqlField.field.setAccessible(true);
            sqlField.field.set(b, ++i[0]);
        };

        allFields.forEach(consumer.wrap());
        System.out.println(b);
    }

    private Stream<SqlField> toSqlField(Field f){
        Predicate<Field> pred = field -> field.getType().isPrimitive() ||
                field.getType().isAssignableFrom(String.class) ||
                field.getType().isAssignableFrom(Timestamp.class) ||
                field.getType().isAssignableFrom(Date.class);
        if(f.isAnnotationPresent(EmbeddedId.class)){
            return Arrays.stream(f.getType()
                    .getDeclaredFields()).filter(pred)
                    .map(fi-> new SqlField.SqlFieldId(fi, fi.getName(), false));
        }
        if(f.isAnnotationPresent(Id.class)){
            return Stream.of(new SqlField.SqlFieldId(f, f.getName(), f.getAnnotation(Id.class).isIdentity()));
        }
        if(f.isAnnotationPresent(ColumnName.class)){
            return Stream.of(new SqlField.SqlFieldExternal(
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

    public class A{
        protected int a;
        protected int b;

        public A(int a, int b) {
            this.a = a;
            this.b = b;
        }

        public A() {
        }
    }

    public class B extends A {
        private int c;

        public B(int a, int b, int c) {
            super(a, b);
            this.c = c;
        }

        public B() {
        }

        @Override
        public String toString() {
            return a + "\n" + b + "\n" + c;
        }
    }
}
