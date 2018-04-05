package org.github.isel.rapper;

import org.github.isel.rapper.utils.ConnectionManager;
import org.github.isel.rapper.utils.MapperRegistry;
import org.github.isel.rapper.utils.UnitOfWork;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.github.isel.rapper.utils.ConnectionManager.DBsPath.TESTDB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataMapperTests {

    @Before
    public void start(){
        MapperRegistry.addEntry(Person.class, new DataMapper<>(Person.class));
        ConnectionManager manager = ConnectionManager.getConnectionManager(TESTDB);
        UnitOfWork.newCurrent(manager::getConnection);
    }

    @After
    public void finish() throws SQLException {
        UnitOfWork.getCurrent().rollback();
        UnitOfWork.getCurrent().closeConnection();
    }

    @Test
    public void InsertUpdateSelectDeleteTest(){
        Person p = new Person(123, "abc", new Timestamp(1969, 6, 9, 16, 04, 30,30), 0);
        DataMapper<Person, Integer> mapper = MapperRegistry.getMapper(Person.class);
        mapper.insert(p);
        System.out.println(mapper.getById(123).join());
        mapper.update(new Person(p.getNif(), "def", p.getBirthday(), p.getVersion()));
        System.out.println(mapper.getById(123).join());
    }

    @Test
    public void findWhere() {
    }

    @Test
    public void getById() {
    }

    @Test
    public void getAll() {
    }

    @Test
    public void insert() throws SQLException {
        //Arrange
        DataMapper<Person, Integer> mapper = MapperRegistry.getMapper(Person.class);
        Person p = new Person(123, "abc", new Timestamp(1969, 6, 9, 16, 04, 30,30), 0);

        //Act
        mapper.insert(p);

        //Assert
        PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select * from Person where nif = ?");
        ps.setInt(1, p.getNif());
        ResultSet rs = ps.executeQuery();
        if (rs.next()) assertEquals(p.getNif(), rs.getInt("nif"));
        else fail("Person wasn't inserted in the database");
    }

    @Test
    public void update() {
    }

    @Test
    public void delete() {
    }

    @Test
    public void getSelectQuery() {
    }

    @Test
    public void getInsertQuery() {
    }

    @Test
    public void getUpdateQuery() {
    }

    @Test
    public void getDeleteQuery() {
    }
}
