package org.github.isel.rapper;

import org.github.isel.rapper.domainModel.*;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Arrays;

import static java.lang.System.out;
import static org.junit.Assert.*;

public class QueryTests {
    @Test
    public void shouldObtainQueriesForSimpleEntity(){
        DataMapper<Person, Integer> dataMapper = new DataMapper<>(Person.class);
        DataMapper<Employee, Integer> employeeMapper = new DataMapper<>(Employee.class);

        assertEquals("select nif, name, birthday, CAST(version as bigint) version from Person", dataMapper.getSelectQuery());
        assertEquals("delete from Person where nif = ?", dataMapper.getDeleteQuery());
        assertEquals("insert into Person ( nif, name, birthday ) output CAST(INSERTED.version as bigint) version values ( ?, ?, ? )", dataMapper.getInsertQuery());
        assertEquals("update Person set name = ?, birthday = ? output CAST(INSERTED.version as bigint) version where nif = ? and version = ?", dataMapper.getUpdateQuery());

        assertEquals("select id, name, companyId, companyCid, CAST(version as bigint) version from Employee", employeeMapper.getSelectQuery());
        assertEquals("delete from Employee where id = ?", employeeMapper.getDeleteQuery());
        assertEquals("insert into Employee ( name, companyId, companyCid ) output CAST(INSERTED.version as bigint) version values ( ?, ?, ? )", employeeMapper.getInsertQuery());
        assertEquals("update Employee set name = ?, companyId = ?, companyCid = ? output CAST(INSERTED.version as bigint) version where id = ? and version = ?", employeeMapper.getUpdateQuery());
    }

    @Test
    public void shouldObtainQueriesForEntitiesWithMultiPK(){
        DataMapper<Car, Car.PrimaryPk> dataMapper = new DataMapper<>(Car.class);
        DataMapper<Company, Company.PrimaryKey> companyMapper = new DataMapper<>(Company.class);

        assertEquals("select owner, plate, brand, model, CAST(version as bigint) version from Car", dataMapper.getSelectQuery());
        assertEquals("delete from Car where owner = ? and plate = ?", dataMapper.getDeleteQuery());
        assertEquals("insert into Car ( owner, plate, brand, model ) output CAST(INSERTED.version as bigint) version values ( ?, ?, ?, ? )", dataMapper.getInsertQuery());
        assertEquals("update Car set brand = ?, model = ? output CAST(INSERTED.version as bigint) version where owner = ? and plate = ? and version = ?", dataMapper.getUpdateQuery());

        assertEquals("select id, cid, motto, CAST(version as bigint) version from Company", companyMapper.getSelectQuery());
        assertEquals("delete from Company where id = ? and cid = ?", companyMapper.getDeleteQuery());
        assertEquals("insert into Company ( id, cid, motto ) output CAST(INSERTED.version as bigint) version values ( ?, ?, ? )", companyMapper.getInsertQuery());
        assertEquals("update Company set motto = ? output CAST(INSERTED.version as bigint) version where id = ? and cid = ? and version = ?", companyMapper.getUpdateQuery());
    }

    @Test
    public void shouldObtainQueriesForEntitiesWithInheritance(){
        DataMapper<Student, Integer> studentMapper = new DataMapper<>(Student.class);
        DataMapper<TopStudent, Integer> topStudentMapper = new DataMapper<>(TopStudent.class);

        assertEquals("select nif, topGrade, year, CAST(version as bigint) version from TopStudent", topStudentMapper.getSelectQuery());
        assertEquals("delete from TopStudent where nif = ?", topStudentMapper.getDeleteQuery());
        assertEquals("insert into TopStudent ( nif, topGrade, year ) output CAST(INSERTED.version as bigint) version values ( ?, ?, ? )", topStudentMapper.getInsertQuery());
        assertEquals("update TopStudent set topGrade = ?, year = ? output CAST(INSERTED.version as bigint) version where nif = ? and version = ?", topStudentMapper.getUpdateQuery());

        assertEquals("select nif, studentNumber, CAST(version as bigint) version from Student", studentMapper.getSelectQuery());
        assertEquals("delete from Student where nif = ?", studentMapper.getDeleteQuery());
        assertEquals("insert into Student ( nif, studentNumber ) output CAST(INSERTED.version as bigint) version values ( ?, ? )", studentMapper.getInsertQuery());
        assertEquals("update Student set studentNumber = ? output CAST(INSERTED.version as bigint) version where nif = ? and version = ?", studentMapper.getUpdateQuery());
    }
}
