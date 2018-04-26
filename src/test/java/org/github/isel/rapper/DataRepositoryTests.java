package org.github.isel.rapper;

import org.github.isel.rapper.domainModel.Employee;
import org.github.isel.rapper.domainModel.Person;
import org.github.isel.rapper.domainModel.TopStudent;
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
import java.util.function.Supplier;

import static org.github.isel.rapper.AssertUtils.assertMultipleRows;
import static org.github.isel.rapper.AssertUtils.assertNotFound;
import static org.github.isel.rapper.AssertUtils.assertSingleRow;
import static org.github.isel.rapper.DBStatements.createTables;
import static org.github.isel.rapper.DBStatements.deleteDB;
import static org.github.isel.rapper.DBStatements.populateDB;
import static org.github.isel.rapper.TestUtils.*;
import static org.github.isel.rapper.utils.DBsPath.TESTDB;
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
        ConnectionManager manager = ConnectionManager.getConnectionManager(TESTDB);
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

        assertSingleRow(UnitOfWork.getCurrent(), second.get(), topStudentSelectQuery, getPersonPSConsumer(second.get().getNif()), AssertUtils::assertTopStudent);
    }

    @Test
    public void findAll() {
        topStudentRepository.findAll().join();
        assertEquals(1, topStudentMapperify.getIfindAll().getCount());

        List<TopStudent> second = topStudentRepository.findAll().join();
        assertEquals(2, topStudentMapperify.getIfindAll().getCount());

        assertMultipleRows(UnitOfWork.getCurrent(), second, topStudentSelectQuery.substring(0, topStudentSelectQuery.length()-16), AssertUtils::assertTopStudent, second.size());
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

        ResultSet rs = executeQuery("select CAST(P.version as bigint), CAST(S2.version as bigint), CAST(TS.version as bigint) version from Person P " +
                "inner join Student S2 on P.nif = S2.nif " +
                "inner join TopStudent TS on S2.nif = TS.nif where P.nif = ?", getPersonPSConsumer(454));
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
        ResultSet rs = executeQuery("select CAST(version as bigint) version from Person where nif = ?", getPersonPSConsumer(321));
        list.add(new Person(321, "Maria", new Date(2010, 2, 3), rs.getLong(1)));

        rs = executeQuery("select CAST(version as bigint) version from Person where nif = ?", getPersonPSConsumer(454));
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
        assertNotFound(topStudentSelectQuery, getPersonPSConsumer(454));
    }

    @Test
    public void delete() {
        TopStudent topStudent = new TopStudent(454, null, null, 0, 0, 0, 0, 0, 0);

        boolean success = topStudentRepository.delete(topStudent).join();

        assertTrue(success);

        Optional<TopStudent> optionalTopStudent = topStudentRepository.findById(454).join();
        assertEquals(1, topStudentMapperify.getIfindById().getCount());
        assertFalse(optionalTopStudent.isPresent());
        assertNotFound(topStudentSelectQuery, getPersonPSConsumer(topStudent.getNif()));
    }

    @Test
    public void deleteAll() throws SQLException {
        ConnectionManager connectionManager = ConnectionManager.getConnectionManager(DBsPath.TESTDB);
        SqlSupplier<Connection> connectionSupplier = connectionManager::getConnection;
        UnitOfWork.newCurrent(connectionSupplier.wrap());

        List<Integer> list = new ArrayList<>(2);
        ResultSet rs = executeQuery("select id from Employee where name = ?", getEmployeePSConsumer("Bob"));
        list.add(rs.getInt("id"));

        rs = executeQuery("select id from Employee where name = ?", getEmployeePSConsumer("Charles"));
        list.add(rs.getInt("id"));

        boolean success = employeeRepository.deleteAll(list).join();

        assertTrue(success);

        Optional<Employee> optionalPerson = employeeRepository.findById(321).join();
        assertEquals(3, employeeMapperify.getIfindById().getCount());
        assertFalse(optionalPerson.isPresent());

        Optional<Employee> optionalPerson2 = employeeRepository.findById(454).join();
        assertEquals(4, employeeMapperify.getIfindById().getCount());
        assertFalse(optionalPerson2.isPresent());

        assertNotFound(employeeSelectQuery, getEmployeePSConsumer("Bob"));
        assertNotFound(employeeSelectQuery, getEmployeePSConsumer("Charles"));

    }
}