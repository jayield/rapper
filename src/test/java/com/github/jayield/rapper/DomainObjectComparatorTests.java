package com.github.jayield.rapper;

import com.github.jayield.rapper.connections.ConnectionManager;
import com.github.jayield.rapper.domainModel.Book;
import com.github.jayield.rapper.domainModel.Car;
import com.github.jayield.rapper.domainModel.CarKey;
import com.github.jayield.rapper.domainModel.Employee;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.mapper.Mapper;
import com.github.jayield.rapper.mapper.MapperRegistry;
import com.github.jayield.rapper.mapper.MapperSettings;
import com.github.jayield.rapper.utils.SqlUtils;
import com.github.jayield.rapper.unitofwork.UnitOfWork;
import com.github.jayield.rapper.utils.*;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.URLDecoder;
import java.util.Comparator;
import java.util.Map;

import static org.junit.Assert.*;

public class DomainObjectComparatorTests {

    private final Comparator<Book> bookComparator;
    private final Comparator<Car> carComparator;
    private final DomainObjectComparator<Employee> employeeComparator;
    private final Map<Class, MapperRegistry.Container> repositoryMap;
    private UnitOfWork unit;

    public DomainObjectComparatorTests() throws NoSuchFieldException, IllegalAccessException {
        MapperSettings carSettings = new MapperSettings(Car.class);
        carComparator = new DomainObjectComparator<>(carSettings);

        MapperSettings bookSettings = new MapperSettings(Book.class);
        bookComparator = new DomainObjectComparator<>(bookSettings);

        MapperSettings employeeSettings = new MapperSettings(Employee.class);
        employeeComparator = new DomainObjectComparator<>(employeeSettings);

        Field repositoryMapField = MapperRegistry.class.getDeclaredField("containerMap");
        repositoryMapField.setAccessible(true);
        repositoryMap = (Map<Class, MapperRegistry.Container>) repositoryMapField.get(null);
    }

    @Before
    public void start() {
        repositoryMap.clear();
        ConnectionManager connectionManager = ConnectionManager.getConnectionManager(
                "jdbc:hsqldb:file:"+URLDecoder.decode(this.getClass().getClassLoader().getResource("testdb").getPath())+"/testdb",
                "SA", "");
        unit = new UnitOfWork(connectionManager::getConnection);
        SQLConnection con = connectionManager.getConnection().join();

        SqlUtils.<ResultSet>callbackToPromise(ar -> con.call("{call deleteDB()}", ar)).join();
        SqlUtils.<ResultSet>callbackToPromise(ar -> con.call("{call populateDB()}", ar)).join();
        SqlUtils.callbackToPromise(con::commit).join();
    }

    @After
    public void after() {
        unit.rollback().join();
    }

    @Test
    public void testCompareCars() {
        Mapper<Car, CarKey> carMapper = MapperRegistry.getRepository(Car.class, unit).getMapper();

        Car car1 = carMapper.findById(new CarKey(2, "23we45"))
                .join()
                .orElseThrow(() -> new DataMapperException("Car not found"));

        Car car2 = carMapper.findById(new CarKey(2, "23we45"))
                .join()
                .orElseThrow(() -> new DataMapperException("Car not found"));

        assertNotEquals(car1, car2);

        assertEquals(0, carComparator.compare(car1, car2));
    }

    @Test
    public void testCompareBooks(){
        Mapper<Book, Long> bookMapper = MapperRegistry.getRepository(Book.class, unit).getMapper();

        Book book1 = bookMapper.findWhere(new Pair<>("name", "1001 noites"))
                .join()
                .get(0);

        Book book2 = bookMapper.findWhere(new Pair<>("name", "1001 noites"))
                .join()
                .get(0);

        assertNotEquals(book1, book2);

        assertEquals(0, bookComparator.compare(book1, book2));
    }

    @Test
    public void testCompareEmployees() {
        Mapper<Employee, Integer> bookMapper = MapperRegistry.getRepository(Employee.class, unit).getMapper();

        Employee employee1 = bookMapper.findWhere(new Pair<>("name", "Charles"))
                .join()
                .get(0);

        Employee employee2 = bookMapper.findWhere(new Pair<>("name", "Bob"))
                .join()
                .get(0);

        assertNotEquals(employee1, employee2);

        assertEquals(-1, employeeComparator.compare(employee1, employee2));
    }
}