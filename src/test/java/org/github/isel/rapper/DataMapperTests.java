package org.github.isel.rapper;

import org.github.isel.rapper.utils.ConnectionManager;
import org.github.isel.rapper.utils.MapperRegistry;
import org.github.isel.rapper.utils.UnitOfWork;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;

import static org.github.isel.rapper.utils.ConnectionManager.DBsPath.TESTDB;

public class DataMapperTests {

    @Before
    public void start(){
        MapperRegistry.addEntry(Person.class, new DataMapper<>(Person.class));
        ConnectionManager manager = ConnectionManager.getConnectionManager(TESTDB.toString());
        UnitOfWork.newCurrent(manager::getConnection);
    }

    @After
    public void finish() throws SQLException {
        UnitOfWork.getCurrent().rollback();
        UnitOfWork.getCurrent().closeConnection();
    }

    @Test
    public void InsertUpdateSelectDeleteTest(){
        try {
            Person p = new Person(123, "abc", new Timestamp(1969, 6, 9, 16, 04, 30,30), 0);
            DataMapper<Person, Integer> mapper = MapperRegistry.getMapper(Person.class);
            mapper.insert(p);
            System.out.println(mapper.getById(123).join());
            mapper.update(new Person(p.getNif(), "def", p.getBirthday(), p.getVersion()));
            System.out.println(mapper.getById(123).join());
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void test(){

    }
}
