package com.github.jayield.rapper;

import com.github.jayield.rapper.domainModel.*;
import com.github.jayield.rapper.utils.MapperSettings;
import org.junit.Test;

import static org.junit.Assert.*;

public class QueryTests {
    @Test
    public void shouldObtainQueriesForSimpleEntity(){
        MapperSettings personSettings = new MapperSettings(Person.class);
        MapperSettings employeeSettings = new MapperSettings(Employee.class);

        assertEquals("select C.nif, C.name, C.birthday, CAST(C.version as bigint) Cversion from [Person] C ", personSettings.getSelectQuery());
        assertEquals("delete from [Person] where nif = ?", personSettings.getDeleteQuery());
        assertEquals("insert into [Person] ( nif, name, birthday ) output CAST(INSERTED.version as bigint) version values ( ?, ?, ? )", personSettings.getInsertQuery());
        assertEquals("update [Person] set name = ?, birthday = ? output CAST(INSERTED.version as bigint) version where nif = ? and version = ?", personSettings.getUpdateQuery());

        assertEquals("select C.id, C.name, CAST(C.version as bigint) Cversion, C.companyId, C.companyCid from [Employee] C ", employeeSettings.getSelectQuery());
        assertEquals("delete from [Employee] where id = ?", employeeSettings.getDeleteQuery());
        assertEquals("insert into [Employee] ( name, companyId, companyCid ) output INSERTED.id, CAST(INSERTED.version as bigint) version values ( ?, ?, ? )", employeeSettings.getInsertQuery());
        assertEquals("update [Employee] set name = ?, companyId = ?, companyCid = ? output CAST(INSERTED.version as bigint) version where id = ? and version = ?", employeeSettings.getUpdateQuery());
    }

    @Test
    public void shouldObtainQueriesForEntitiesWithMultiPK(){
        MapperSettings dataMapper = new MapperSettings(Car.class);
        MapperSettings companyMapper = new MapperSettings(Company.class);

        assertEquals("select C.owner, C.plate, C.brand, C.model, CAST(C.version as bigint) Cversion from [Car] C ", dataMapper.getSelectQuery());
        assertEquals("delete from [Car] where owner = ? and plate = ?", dataMapper.getDeleteQuery());
        assertEquals("insert into [Car] ( owner, plate, brand, model ) output CAST(INSERTED.version as bigint) version values ( ?, ?, ?, ? )", dataMapper.getInsertQuery());
        assertEquals("update [Car] set brand = ?, model = ? output CAST(INSERTED.version as bigint) version where owner = ? and plate = ? and version = ?", dataMapper.getUpdateQuery());

        assertEquals("select C.id, C.cid, C.motto, CAST(C.version as bigint) Cversion from [Company] C ", companyMapper.getSelectQuery());
        assertEquals("delete from [Company] where id = ? and cid = ?", companyMapper.getDeleteQuery());
        assertEquals("insert into [Company] ( id, cid, motto ) output CAST(INSERTED.version as bigint) version values ( ?, ?, ? )", companyMapper.getInsertQuery());
        assertEquals("update [Company] set motto = ? output CAST(INSERTED.version as bigint) version where id = ? and cid = ? and version = ?", companyMapper.getUpdateQuery());
    }

    @Test
    public void shouldObtainQueriesForEntitiesWithInheritance(){
        MapperSettings studentMapper = new MapperSettings(Student.class);
        MapperSettings topStudentMapper = new MapperSettings(TopStudent.class);

        assertEquals("select P1.studentNumber, CAST(P1.version as bigint) P1version, P2.name, P2.birthday, CAST(P2.version as bigint) P2version, P2.nif, C.topGrade, C.year, " +
                "CAST(C.version as bigint) Cversion from [TopStudent] C inner join [Student] P1 on C.nif = P1.nif inner join [Person] P2 on P1.nif = P2.nif ", topStudentMapper.getSelectQuery());
        assertEquals("delete from [TopStudent] where nif = ?", topStudentMapper.getDeleteQuery());
        assertEquals("insert into [TopStudent] ( nif, topGrade, year ) output CAST(INSERTED.version as bigint) version values ( ?, ?, ? )", topStudentMapper.getInsertQuery());
        assertEquals("update [TopStudent] set topGrade = ?, year = ? output CAST(INSERTED.version as bigint) version where nif = ? and version = ?", topStudentMapper.getUpdateQuery());

        assertEquals("select P1.name, P1.birthday, CAST(P1.version as bigint) P1version, P1.nif, C.studentNumber, CAST(C.version as bigint) Cversion from [Student] C " +
                "inner join [Person] P1 on C.nif = P1.nif ", studentMapper.getSelectQuery());
        assertEquals("delete from [Student] where nif = ?", studentMapper.getDeleteQuery());
        assertEquals("insert into [Student] ( nif, studentNumber ) output CAST(INSERTED.version as bigint) version values ( ?, ? )", studentMapper.getInsertQuery());
        assertEquals("update [Student] set studentNumber = ? output CAST(INSERTED.version as bigint) version where nif = ? and version = ?", studentMapper.getUpdateQuery());
    }
}
