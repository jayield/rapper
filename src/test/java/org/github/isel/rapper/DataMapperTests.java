package org.github.isel.rapper;

import javafx.util.Pair;
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
import java.util.Optional;

import static org.github.isel.rapper.utils.ConnectionManager.DBsPath.TESTDB;
import static org.junit.Assert.*;

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
    public void findWhere() throws SQLException {
        DataMapper<Person, Integer> mapper = MapperRegistry.getMapper(Person.class);

        List<Person> people = mapper.findWhere(new Pair<>("name", "Jose")).join();

        PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select nif, name, birthday, CAST(version as bigint) version from Person where name = ?");
        ps.setString(1, "Jose");
        ResultSet rs = ps.executeQuery();
        while (rs.next())
            assertPerson(people.remove(0), rs);
    }

    @Test
    public void getById() throws SQLException {
        DataMapper<Person, Integer> mapper = MapperRegistry.getMapper(Person.class);
        int nif = 321;
        String errorMessage = "Person wasn't selected from the database";
        PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select nif, name, birthday, CAST(version as bigint) version from Person where nif = ?");
        ps.setInt(1, nif);
        ResultSet rs = ps.executeQuery();

        Optional<Person> optionalPerson = mapper.getById(nif).join();

        if(optionalPerson.isPresent() && rs.next())
            assertPerson(optionalPerson.get(), rs);
        else fail(errorMessage);
    }

    @Test
    public void getAll() throws SQLException {
        DataMapper<Person, Integer> mapper = MapperRegistry.getMapper(Person.class);
        String errorMessage = "People weren't selected from the database";

        List<Person> people = mapper.getAll().join();

        PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select nif, name, birthday, CAST(version as bigint) version from Person");
        ResultSet rs = ps.executeQuery();
        if (rs.next())
            assertPerson(people.get(0), rs);
        else fail(errorMessage);
    }

    private void assertPerson(Person person, ResultSet rs) throws SQLException {
        assertEquals(person.getNif(), rs.getInt("nif"));
        assertEquals(person.getName(), rs.getString("name"));
        assertEquals(person.getBirthday(), rs.getDate("birthday"));
        assertEquals(person.getVersion(), rs.getLong("version"));
    }

    @Test
    public void insert() throws SQLException {
        //Arrange
        DataMapper<Person, Integer> mapper = MapperRegistry.getMapper(Person.class);
        Person person = new Person(123, "abc", new Date(1969, 6, 9), 0);

        //Act
        mapper.insert(person);

        //Assert
        PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select nif, name, birthday, CAST(version as bigint) version from Person where nif = ?");
        ps.setInt(1, person.getNif());
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            assertPerson(person, rs);
        }
        else fail("Person wasn't inserted in the database");
    }

    @Test
    public void update() throws SQLException {
        DataMapper<Person, Integer> mapper = MapperRegistry.getMapper(Person.class);
        Person person = new Person(321, "Maria", new Date(2010, 2, 3), 0);

        mapper.update(person);

        PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select nif, name, birthday, CAST(version as bigint) version from Person where nif = ?");
        ps.setInt(1, person.getNif());
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            assertPerson(person, rs);
        }
        else fail("Person wasn't updated in the database");
    }

    @Test
    public void delete() throws SQLException {
        DataMapper<Person, Integer> mapper = MapperRegistry.getMapper(Person.class);
        Person person = new Person(321, null, null, 0);

        mapper.delete(person);

        PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select nif, name, birthday, CAST(version as bigint) version from Person where nif = ?");
        ps.setInt(1, person.getNif());
        ResultSet rs = ps.executeQuery();
        assertFalse(rs.next());
    }

    @Test
    public void getSelectQuery() {
        DataMapper<Person, Integer> mapper = MapperRegistry.getMapper(Person.class);
        assertEquals("select nif, name, birthday, CAST(version as bigint) version from Person", mapper.getSelectQuery());
    }

    @Test
    public void getInsertQuery() {
        DataMapper<Person, Integer> mapper = MapperRegistry.getMapper(Person.class);
        assertEquals("insert into Person ( nif, name, birthday ) output CAST(INSERTED.version as bigint) version values ( ?, ?, ? )", mapper.getInsertQuery());
    }

    @Test
    public void getUpdateQuery() {
        DataMapper<Person, Integer> mapper = MapperRegistry.getMapper(Person.class);
        assertEquals("update Person set nif = ?, name = ?, birthday = ? output CAST(INSERTED.version as bigint) version where nif = ?", mapper.getUpdateQuery());
    }

    @Test
    public void getDeleteQuery() {
        DataMapper<Person, Integer> mapper = MapperRegistry.getMapper(Person.class);
        assertEquals("delete from Person where nif = ?", mapper.getDeleteQuery());
    }
}
