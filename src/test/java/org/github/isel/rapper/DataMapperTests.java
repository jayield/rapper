package org.github.isel.rapper;

import org.github.isel.rapper.utils.ConnectionManager;
import org.github.isel.rapper.utils.MapperRegistry;
import org.github.isel.rapper.utils.UnitOfWork;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;

import static org.github.isel.rapper.utils.ConnectionManager.DBsPath.TESTDB;

public class DataMapperTests {

    @Before
    public void start(){
        MapperRegistry.addEntry(Person.class, new DataMapper<>(Person.class, Integer.class));
        ConnectionManager manager = ConnectionManager.getConnectionManager(TESTDB.toString());
        UnitOfWork.newCurrent(manager::getConnection);
    }

    @After
//    public void finish() throws SQLException {
//        UnitOfWork.getCurrent().rollback();
//        UnitOfWork.getCurrent().closeConnection();
//    }

    @Test
    public void InsertUpdateSelectDeleteTest(){
        Person p = new Person(123, "abc", new Date(1969, 6, 9));
        DataMapper<Person, Integer> mapper = MapperRegistry.getMapper(Person.class).get();
        mapper.insert(p);
        System.out.println(mapper.getById(123));
    }
}
