package com.github.jayield.rapper;

import com.github.jayield.rapper.domainModel.*;
import com.github.jayield.rapper.mapper.MapperSettings;
import org.junit.Test;

import static org.junit.Assert.*;

public class QueryTests {
    @Test
    public void shouldObtainQueriesForSimpleEntity(){
        MapperSettings personSettings = new MapperSettings(Person.class);
        MapperSettings employeeSettings = new MapperSettings(Employee.class);

        assertEquals("select C.nif, C.name, C.birthday, CAST(C.version as bigint) Cversion from Person C ", personSettings.getSelectQuery());
        assertEquals("delete from Person where nif = ?", personSettings.getDeleteQuery());
        assertEquals("insert into Person ( nif, name, birthday ) values ( ?, ?, ? )", personSettings.getInsertQuery());
        assertEquals("update Person set name = ?, birthday = ? where nif = ? and version = ?", personSettings.getUpdateQuery());

        assertEquals("select C.id, C.name, CAST(C.version as bigint) Cversion, C.companyId, C.companyCid from Employee C ", employeeSettings.getSelectQuery());
        assertEquals("delete from Employee where id = ?", employeeSettings.getDeleteQuery());
        assertEquals("insert into Employee ( name, companyId, companyCid ) values ( ?, ?, ? )", employeeSettings.getInsertQuery());
        assertEquals("update Employee set name = ?, companyId = ?, companyCid = ? where id = ? and version = ?", employeeSettings.getUpdateQuery());
    }

    @Test
    public void shouldObtainQueriesForEntitiesWithMultiPK(){
        MapperSettings carSettings = new MapperSettings(Car.class);
        MapperSettings companySettings = new MapperSettings(Company.class);

        assertEquals("select C.owner, C.plate, C.brand, C.model, CAST(C.version as bigint) Cversion from Car C ", carSettings.getSelectQuery());
        assertEquals("delete from Car where owner = ? and plate = ?", carSettings.getDeleteQuery());
        assertEquals("insert into Car ( owner, plate, brand, model ) values ( ?, ?, ?, ? )", carSettings.getInsertQuery());
        assertEquals("update Car set brand = ?, model = ? where owner = ? and plate = ? and version = ?", carSettings.getUpdateQuery());

        assertEquals("select C.id, C.cid, C.motto, CAST(C.version as bigint) Cversion from Company C ", companySettings.getSelectQuery());
        assertEquals("delete from Company where id = ? and cid = ?", companySettings.getDeleteQuery());
        assertEquals("insert into Company ( id, cid, motto ) values ( ?, ?, ? )", companySettings.getInsertQuery());
        assertEquals("update Company set motto = ? where id = ? and cid = ? and version = ?", companySettings.getUpdateQuery());
    }

    @Test
    public void shouldObtainQueriesForEntitiesWithInheritance(){
        MapperSettings studentMapper = new MapperSettings(Student.class);
        MapperSettings topStudentMapper = new MapperSettings(TopStudent.class);

        assertEquals("select P1.studentNumber, CAST(P1.version as bigint) P1version, P2.name, P2.birthday, CAST(P2.version as bigint) P2version, P2.nif, C.topGrade, C.year, " +
                "CAST(C.version as bigint) Cversion from TopStudent C inner join Student P1 on C.nif = P1.nif inner join Person P2 on P1.nif = P2.nif ", topStudentMapper.getSelectQuery());
        assertEquals("delete from TopStudent where nif = ?", topStudentMapper.getDeleteQuery());
        assertEquals("insert into TopStudent ( nif, topGrade, year ) values ( ?, ?, ? )", topStudentMapper.getInsertQuery());
        assertEquals("update TopStudent set topGrade = ?, year = ? where nif = ? and version = ?", topStudentMapper.getUpdateQuery());

        assertEquals("select P1.name, P1.birthday, CAST(P1.version as bigint) P1version, P1.nif, C.studentNumber, CAST(C.version as bigint) Cversion from Student C " +
                "inner join Person P1 on C.nif = P1.nif ", studentMapper.getSelectQuery());
        assertEquals("delete from Student where nif = ?", studentMapper.getDeleteQuery());
        assertEquals("insert into Student ( nif, studentNumber ) values ( ?, ? )", studentMapper.getInsertQuery());
        assertEquals("update Student set studentNumber = ? where nif = ? and version = ?", studentMapper.getUpdateQuery());
    }

    @Test
    public void shouldObtainQueriesForEntitiesWithoutVersion(){
        MapperSettings dogSettings = new MapperSettings(Dog.class);

        assertEquals("select C.name, C.race, C.age from Dog C ", dogSettings.getSelectQuery());
        assertEquals("delete from Dog where name = ? and race = ?", dogSettings.getDeleteQuery());
        assertEquals("insert into Dog ( name, race, age ) values ( ?, ?, ? )", dogSettings.getInsertQuery());
        assertEquals("update Dog set age = ? where name = ? and race = ?", dogSettings.getUpdateQuery());
    }
}
