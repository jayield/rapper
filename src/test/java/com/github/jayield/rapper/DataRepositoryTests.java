package com.github.jayield.rapper;

import com.github.jayield.rapper.domainModel.Employee;
import com.github.jayield.rapper.domainModel.Person;
import com.github.jayield.rapper.domainModel.TopStudent;
import com.github.jayield.rapper.utils.*;
import org.github.isel.rapper.utils.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.*;

public class DataRepositoryTests {

    private DataRepository<TopStudent, Integer> topStudentRepository;
    private DataRepository<Person, Integer> personRepository;
    private DataRepository<Employee, Integer> employeeRepository;

    private Mapperify<TopStudent, Integer> topStudentMapperify;
    private Mapperify<Person, Integer> personMapperify;
    private Mapperify<Employee, Integer> employeeMapperify;

    private Map<Class, DataRepository> repositoryMap;

    private final DataMapper<TopStudent, Integer> topStudentMapper;
    private final DataMapper<Person, Integer> personMapper;
    private final DataMapper<Employee, Integer> employeeMapper;

    public DataRepositoryTests() throws NoSuchFieldException, IllegalAccessException {
        topStudentMapper = new DataMapper<>(TopStudent.class);
        personMapper = new DataMapper<>(Person.class);
        employeeMapper = new DataMapper<>(Employee.class);

        Field repositoryMapField = MapperRegistry.class.getDeclaredField("repositoryMap");
        repositoryMapField.setAccessible(true);
        repositoryMap = (Map<Class, DataRepository>) repositoryMapField.get(new MapperRegistry());
    }

    @Before
    public void before() throws SQLException {
        ConnectionManager manager = ConnectionManager.getConnectionManager(DBsPath.TESTDB);
        Connection con = manager.getConnection();
        con.prepareCall("{call deleteDB}").execute();
        con.prepareCall("{call populateDB}").execute();
        con.prepareStatement("delete from EmployeeJunior").executeUpdate();
        con.commit();
        /*createTables(con);
        deleteDB(con);
        populateDB(con);*/

        topStudentMapperify = new Mapperify<>(topStudentMapper);
        topStudentRepository = new DataRepository<>(topStudentMapperify);

        personMapperify = new Mapperify<>(personMapper);
        personRepository = new DataRepository<>(personMapperify);

        employeeMapperify = new Mapperify<>(employeeMapper);
        employeeRepository = new DataRepository<>(employeeMapperify);

        repositoryMap.put(TopStudent.class, topStudentRepository);
        repositoryMap.put(Person.class, personRepository);
        repositoryMap.put(Employee.class, employeeRepository);
    }

    @After
    public void after(){
        repositoryMap.clear();
    }

    @Test
    public void findWhere(){
    }

    @Test
    public void findById() {
        Optional<TopStudent> first = topStudentRepository.findById(454).join();
        assertEquals(1, topStudentMapperify.getIfindById().getCount());

        Optional<TopStudent> second = topStudentRepository.findById(454).join();
        assertEquals(1, topStudentMapperify.getIfindById().getCount());

        assertEquals(first.get(), second.get());

        AssertUtils.assertSingleRow(UnitOfWork.getCurrent(), second.get(), TestUtils.topStudentSelectQuery, TestUtils.getPersonPSConsumer(second.get().getNif()), AssertUtils::assertTopStudent);
    }

    @Test
    public void findAll() {
        topStudentRepository.findAll().join();
        assertEquals(1, topStudentMapperify.getIfindAll().getCount());

        List<TopStudent> second = topStudentRepository.findAll().join();
        assertEquals(2, topStudentMapperify.getIfindAll().getCount());

        AssertUtils.assertMultipleRows(UnitOfWork.getCurrent(), second, TestUtils.topStudentSelectQuery.substring(0, TestUtils.topStudentSelectQuery.length()-16), AssertUtils::assertTopStudent, second.size());
    }

    @Test
    public void create() {
        //Arrange
        TopStudent topStudent = new TopStudent(456, "Manel", new Date(2020, 12, 1), 0, 1, 20, 2016, 0, 0);

        //Act
        Boolean success = topStudentRepository.create(topStudent).join();

        //Assert
        assertEquals(true, success);

        Optional<TopStudent> first = topStudentRepository.findById(456).join();
        assertEquals(0, topStudentMapperify.getIfindById().getCount());

        assertEquals(first.get(), topStudent);
    }

    @Test
    public void createAll() {
        //Arrange
        List<TopStudent> list = new ArrayList<>(2);
        list.add(new TopStudent(456, "Manel", new Date(2020, 12, 1), 0, 1, 20, 2016, 0, 0));
        list.add(new TopStudent(457, "Maria", null, 0, 2, 18, 2010, 0, 0));

        //Act
        Boolean success = topStudentRepository.createAll(list).join();

        //Assert
        assertEquals(true, success);

        Optional<TopStudent> first = topStudentRepository.findById(456).join();
        assertEquals(0, topStudentMapperify.getIfindById().getCount());
        Optional<TopStudent> second = topStudentRepository.findById(457).join();
        assertEquals(0, topStudentMapperify.getIfindById().getCount());

        assertEquals(first.get(), list.get(0));
        assertEquals(second.get(), list.get(1));
    }

    @Test
    public void update() throws SQLException {
        ConnectionManager connectionManager = ConnectionManager.getConnectionManager(DBsPath.TESTDB);
        SqlSupplier<Connection> connectionSupplier = connectionManager::getConnection;
        UnitOfWork.newCurrent(connectionSupplier.wrap());

        ResultSet rs = TestUtils.executeQuery("select CAST(P.version as bigint), CAST(S2.version as bigint), CAST(TS.version as bigint) version from Person P " +
                "inner join Student S2 on P.nif = S2.nif " +
                "inner join TopStudent TS on S2.nif = TS.nif where P.nif = ?", TestUtils.getPersonPSConsumer(454));
        TopStudent topStudent = new TopStudent(454, "Carlos", new Date(2010, 6, 3), rs.getLong(2),
                4, 6, 7, rs.getLong(3), rs.getLong(1));

        boolean success = topStudentRepository.update(topStudent).join();

        assertTrue(success);

        Optional<TopStudent> first = topStudentRepository.findById(454).join();
        assertEquals(0, topStudentMapperify.getIfindById().getCount());
        topStudentRepository.findById(454).join();
        assertEquals(0, topStudentMapperify.getIfindById().getCount());

        assertEquals(first.get(), topStudent);
    }

    @Test
    public void updateAll() throws SQLException {
        ConnectionManager connectionManager = ConnectionManager.getConnectionManager(DBsPath.TESTDB);
        SqlSupplier<Connection> connectionSupplier = connectionManager::getConnection;
        UnitOfWork.newCurrent(connectionSupplier.wrap());

        List<Person> list = new ArrayList<>(2);
        ResultSet rs = TestUtils.executeQuery("select CAST(version as bigint) version from Person where nif = ?", TestUtils.getPersonPSConsumer(321));
        list.add(new Person(321, "Maria", new Date(2010, 2, 3), rs.getLong(1)));

        rs = TestUtils.executeQuery("select CAST(version as bigint) version from Person where nif = ?", TestUtils.getPersonPSConsumer(454));
        list.add(new Person(454, "Ze Miguens", new Date(1080, 2, 4), rs.getLong(1)));

        boolean success = personRepository.updateAll(list).join();

        assertTrue(success);

        Optional<Person> first = personRepository.findById(321).join();
        assertEquals(0, personMapperify.getIfindById().getCount());
        Optional<Person> second = personRepository.findById(454).join();
        assertEquals(0, personMapperify.getIfindById().getCount());

        assertEquals(first.get(), list.get(0));
        assertEquals(second.get(), list.get(1));
    }

    @Test
    public void deleteById() {
        boolean success = topStudentRepository.deleteById(454).join();

        assertTrue(success);

        Optional<TopStudent> optionalTopStudent = topStudentRepository.findById(454).join();
        assertEquals(2, topStudentMapperify.getIfindById().getCount());
        assertFalse(optionalTopStudent.isPresent());
        AssertUtils.assertNotFound(TestUtils.topStudentSelectQuery, TestUtils.getPersonPSConsumer(454));
    }

    @Test
    public void delete() {
        TopStudent topStudent = new TopStudent(454, null, null, 0, 0, 0, 0, 0, 0);

        boolean success = topStudentRepository.delete(topStudent).join();

        assertTrue(success);

        Optional<TopStudent> optionalTopStudent = topStudentRepository.findById(454).join();
        assertEquals(1, topStudentMapperify.getIfindById().getCount());
        assertFalse(optionalTopStudent.isPresent());
        AssertUtils.assertNotFound(TestUtils.topStudentSelectQuery, TestUtils.getPersonPSConsumer(topStudent.getNif()));
    }

    @Test
    public void deleteAll() throws SQLException {
        ConnectionManager connectionManager = ConnectionManager.getConnectionManager(DBsPath.TESTDB);
        SqlSupplier<Connection> connectionSupplier = connectionManager::getConnection;
        UnitOfWork.newCurrent(connectionSupplier.wrap());

        List<Integer> list = new ArrayList<>(2);
        ResultSet rs = TestUtils.executeQuery("select id from Employee where name = ?", TestUtils.getEmployeePSConsumer("Bob"));
        list.add(rs.getInt("id"));

        rs = TestUtils.executeQuery("select id from Employee where name = ?", TestUtils.getEmployeePSConsumer("Charles"));
        list.add(rs.getInt("id"));

        boolean success = employeeRepository.deleteAll(list).join();

        assertTrue(success);

        Optional<Employee> optionalPerson = employeeRepository.findById(321).join();
        assertEquals(3, employeeMapperify.getIfindById().getCount());
        assertFalse(optionalPerson.isPresent());

        Optional<Employee> optionalPerson2 = employeeRepository.findById(454).join();
        assertEquals(4, employeeMapperify.getIfindById().getCount());
        assertFalse(optionalPerson2.isPresent());

        AssertUtils.assertNotFound(TestUtils.employeeSelectQuery, TestUtils.getEmployeePSConsumer("Bob"));
        AssertUtils.assertNotFound(TestUtils.employeeSelectQuery, TestUtils.getEmployeePSConsumer("Charles"));

    }
}