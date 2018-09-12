package com.github.jayield.rapper.mapper.externals;

import com.github.jayield.rapper.connections.ConnectionManager;
import com.github.jayield.rapper.domainModel.Dog;
import com.github.jayield.rapper.domainModel.Employee;
import com.github.jayield.rapper.mapper.DataMapper;
import com.github.jayield.rapper.mapper.MapperRegistry;
import com.github.jayield.rapper.mapper.conditions.EqualAndCondition;
import com.github.jayield.rapper.utils.SqlUtils;
import com.github.jayield.rapper.unitofwork.UnitOfWork;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URLDecoder;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExternalsHandlerTests {

    @Before
    public void before() {
        assertEquals(0, UnitOfWork.getNumberOfOpenConnections().get());
        ConnectionManager manager = ConnectionManager.getConnectionManager(
                "jdbc:hsqldb:file:" + URLDecoder.decode(this.getClass().getClassLoader().getResource("testdb").getPath()) + "/testdb",
                "SA",
                ""
        );

        UnitOfWork unit = new UnitOfWork(manager::getConnection);
        SQLConnection con = unit.getConnection().join();
        SqlUtils.<ResultSet>callbackToPromise(ar -> con.call("{call deleteDB()}", ar)).join();
        SqlUtils.<ResultSet>callbackToPromise(ar -> con.call("{call populateDB()}", ar)).join();
        unit.commit().join();
    }

    @After
    public void after() {
        assertEquals(0, UnitOfWork.getNumberOfOpenConnections().get());
    }

    @Test
    public void testNullEmbeddedIdForeign() {
        UnitOfWork unit = new UnitOfWork();
        DataMapper<Employee, Integer> mapper = MapperRegistry.getMapper(Employee.class, unit);
        List<Employee> employees = mapper.find(new EqualAndCondition<>("name", "Maria")).join();
        assertEquals(1, employees.size());

        Employee employee = employees.get(0);

        unit.commit().join();
    }

    @Test
    public void testNullIdForeign() {
        UnitOfWork unit = new UnitOfWork();
        DataMapper<Dog, Dog.DogPK> mapper = MapperRegistry.getMapper(Dog.class, unit);
        List<Dog> dogs = mapper.find(new EqualAndCondition<>("name", "Doggy")).join();
        assertEquals(1, dogs.size());

        Dog dog = dogs.get(0);

        unit.commit().join();
    }
}
