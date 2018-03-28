package org.github.isel.rapper;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Arrays;

import static java.lang.System.out;

public class QueryTests {
    @Test
    public void shouldObtainQueriesForSimpleEntity(){
        DataMapper<Person, Integer> dataMapper = new DataMapper<>(Person.class);

        Assert.assertEquals("select nif, name, birthday from Person", dataMapper.getSelectQuery());
        Assert.assertEquals("delete from Person where nif = ?", dataMapper.getDeleteQuery());
        Assert.assertEquals("insert into Person ( nif, name, birthday ) values ( ?, ?, ? )", dataMapper.getInsertQuery());
        Assert.assertEquals("update Person set name = ?, birthday = ? where nif = ?", dataMapper.getUpdateQuery());
    }

    @Test
    public void shouldObtainQueriesForEntitiesWithMultiPK(){
        DataMapper<Car, Car.PrimaryPk> dataMapper = new DataMapper<>(Car.class);

        Assert.assertEquals("select owner, plate, brand, model from Car", dataMapper.getSelectQuery());
        Assert.assertEquals("delete from Car where owner = ? and plate = ?", dataMapper.getDeleteQuery());
        Assert.assertEquals("insert into Car ( owner, plate, brand, model ) values ( ?, ?, ?, ? )", dataMapper.getInsertQuery());
        Assert.assertEquals("update Car set brand = ?, model = ? where owner = ? and plate = ?", dataMapper.getUpdateQuery());
    }

    @Test
    public void shouldObtainQueriesForEntitiesWithInheritance(){
        DataMapper<Student, Integer> dataMapper = new DataMapper<>(Student.class);

        Assert.assertEquals("select nif studentNumber from Student", dataMapper.getSelectQuery());
        Assert.assertEquals("delete from Student where nif = ?", dataMapper.getDeleteQuery());
        Assert.assertEquals("insert into Student ( nif, studentNumber ) values ( ?, ? )", dataMapper.getInsertQuery());
        Assert.assertEquals("update Student set studentNumber = ? where nif = ?", dataMapper.getUpdateQuery());
    }

    @Test
    public void test() throws NoSuchMethodException {
        Class type = Person.class;

        Field[] fields = type.getDeclaredFields();

        Constructor c = type.getConstructor(Arrays.stream(fields).map(Field::getType).toArray(Class[]::new));



        Arrays.stream(type.getConstructors()[0].getParameters()).map(p->p.getParameterizedType()).forEach(out::println);

    }



}
