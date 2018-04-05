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

    private void assertPerson(Person person, ResultSet rs, String errorMessage) throws SQLException {
        if (rs.next()) {
            assertEquals(person.getNif(), rs.getInt("nif"));
            assertEquals(person.getName(), rs.getString("name"));
            //assertEquals(p.getBirthday(), rs.getTimestamp("birthday"));
            assertEquals(person.getVersion(), rs.getLong("version"));
        }
        else fail(errorMessage);
    }

    @Test
    public void insert() throws SQLException {
        //Arrange
        DataMapper<Person, Integer> mapper = MapperRegistry.getMapper(Person.class);
        Person person = new Person(123, "abc", new Timestamp(1969, 6, 9, 16, 04, 30,30), 0);

        //Act
        mapper.insert(person);

        //Assert
        PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select nif, name, birthday, CAST(version as bigint) version from Person where nif = ?");
        ps.setInt(1, person.getNif());
        ResultSet rs = ps.executeQuery();
        assertPerson(person, rs, "Person wasn't inserted in the database");
    }

    @Test
    public void update() throws SQLException {
        DataMapper<Person, Integer> mapper = MapperRegistry.getMapper(Person.class);
        Person person = new Person(321, "Maria", new Timestamp(21312312), 0);

        mapper.update(person);

        PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select nif, name, birthday, CAST(version as bigint) version from Person where nif = ?");
        ps.setInt(1, person.getNif());
        ResultSet rs = ps.executeQuery();
        assertPerson(person, rs, "Person wasn't updated in the database");
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
