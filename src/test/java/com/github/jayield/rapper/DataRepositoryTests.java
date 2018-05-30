package com.github.jayield.rapper;

import com.github.jayield.rapper.domainModel.*;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.utils.*;
import javafx.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.URLDecoder;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static com.github.jayield.rapper.AssertUtils.*;
import static com.github.jayield.rapper.TestUtils.*;
import static org.junit.Assert.*;

public class DataRepositoryTests {
    private DataRepository<TopStudent, Integer> topStudentRepo;
    private DataRepository<Person, Integer> personRepo;
    private DataRepository<Employee, Integer> employeeRepo;
    private DataRepository<Company, Company.PrimaryKey> companyRepo;
    private DataRepository<Book, Long> bookRepo;
    private DataRepository<Author, Long> authorRepo;

    private Mapperify<TopStudent, Integer> topStudentMapperify;
    private Mapperify<Person, Integer> personMapperify;
    private Mapperify<Employee, Integer> employeeMapperify;
    private Mapperify<Company, Company.PrimaryKey> companyMapperify;
    private Mapperify<Book, Long> bookMapperify;
    private Mapperify<Author, Long> authorMapperify;

    private Map<Class, MapperRegistry.Container> repositoryMap;

    private Connection con;
    public DataRepositoryTests() throws NoSuchFieldException, IllegalAccessException {
        Field repositoryMapField = MapperRegistry.class.getDeclaredField("repositoryMap");
        repositoryMapField.setAccessible(true);
        repositoryMap = (Map<Class, MapperRegistry.Container>) repositoryMapField.get(null);
    }

    @Before
    public void before() throws SQLException {
        UnitOfWork.removeCurrent();
        repositoryMap.clear();
        ConnectionManager manager = ConnectionManager.getConnectionManager(
                "jdbc:hsqldb:file:"+URLDecoder.decode(this.getClass().getClassLoader().getResource("testdb").getPath())+"/testdb",
                "SA", "");
        con = manager.getConnection();
        con.prepareCall("{call deleteDB()}").execute();
        con.prepareCall("{call populateDB()}").execute();
        con.commit();

        MapperSettings topStudentSettings = new MapperSettings(TopStudent.class);
        MapperSettings personSettings = new MapperSettings(Person.class);
        MapperSettings employeeSettings = new MapperSettings(Employee.class);
        MapperSettings companySettings = new MapperSettings(Company.class);
        MapperSettings bookSettings = new MapperSettings(Book.class);
        MapperSettings authorSettings = new MapperSettings(Author.class);

        ExternalsHandler<TopStudent, Integer> topStudentExternal = new ExternalsHandler<>(topStudentSettings);
        ExternalsHandler<Person, Integer> personExternal = new ExternalsHandler<>(personSettings);
        ExternalsHandler<Employee, Integer> employeeExternal = new ExternalsHandler<>(employeeSettings);
        ExternalsHandler<Company, Company.PrimaryKey> companyExternal = new ExternalsHandler<>(companySettings);
        ExternalsHandler<Book, Long> bookExternal = new ExternalsHandler<>(bookSettings);
        ExternalsHandler<Author, Long> authorExternal = new ExternalsHandler<>(authorSettings);

        DataMapper<TopStudent, Integer> topStudentMapper = new DataMapper<>(TopStudent.class, topStudentSettings);
        DataMapper<Person, Integer> personMapper = new DataMapper<>(Person.class, personSettings);
        DataMapper<Employee, Integer> employeeMapper = new DataMapper<>(Employee.class, employeeSettings);
        DataMapper<Company, Company.PrimaryKey> companyMapper = new DataMapper<>(Company.class, companySettings);
        DataMapper<Book, Long> bookMapper = new DataMapper<>(Book.class, bookSettings);
        DataMapper<Author, Long> authorMapper = new DataMapper<>(Author.class, authorSettings);

        topStudentMapperify = new Mapperify<>(topStudentMapper);
        personMapperify = new Mapperify<>(personMapper);
        employeeMapperify = new Mapperify<>(employeeMapper);
        companyMapperify = new Mapperify<>(companyMapper);
        bookMapperify = new Mapperify<>(bookMapper);
        authorMapperify = new Mapperify<>(authorMapper);

        Comparator<TopStudent> topStudentComparator = new DomainObjectComparator<>(topStudentSettings);
        Comparator<Person> personComparator = new DomainObjectComparator<>(personSettings);
        Comparator<Employee> employeeComparator = new DomainObjectComparator<>(employeeSettings);
        Comparator<Company> companyComparator = new DomainObjectComparator<>(companySettings);
        Comparator<Book> bookComparator = new DomainObjectComparator<>(bookSettings);
        Comparator<Author> authorComparator = new DomainObjectComparator<>(authorSettings);

        topStudentRepo = new DataRepository<>(TopStudent.class, Integer.class, topStudentMapperify, topStudentExternal, topStudentComparator);
        personRepo = new DataRepository<>(Person.class, Integer.class, personMapperify, personExternal, personComparator);
        employeeRepo = new DataRepository<>(Employee.class, Integer.class, employeeMapperify, employeeExternal, employeeComparator);
        companyRepo = new DataRepository<>(Company.class, Company.PrimaryKey.class, companyMapperify, companyExternal, companyComparator);
        bookRepo = new DataRepository<>(Book.class, Long.class, bookMapperify, bookExternal, bookComparator);
        authorRepo = new DataRepository<>(Author.class, Long.class, authorMapperify, authorExternal, authorComparator);

        repositoryMap.put(TopStudent.class, new MapperRegistry.Container<>(topStudentSettings, topStudentExternal, topStudentRepo, topStudentMapper));
        repositoryMap.put(Person.class, new MapperRegistry.Container<>(personSettings, personExternal, personRepo, personMapper));
        repositoryMap.put(Employee.class, new MapperRegistry.Container<>(employeeSettings, employeeExternal, employeeRepo, employeeMapper));
        repositoryMap.put(Company.class, new MapperRegistry.Container<>(companySettings, companyExternal, companyRepo, companyMapper));
        repositoryMap.put(Author.class, new MapperRegistry.Container<>(authorSettings, authorExternal, authorRepo, authorMapper));
    }

    @After
    public void after() throws SQLException {
        con.rollback();
        con.close();
    }

    //-----------------------------------Find-----------------------------------//

    @Test
    public void testfindById() {
        Optional<TopStudent> first = topStudentRepo.findById(454).join();
        assertEquals(1, topStudentMapperify.getIfindById().getCount());

        Optional<TopStudent> second = topStudentRepo.findById(454).join();
        assertEquals(1, topStudentMapperify.getIfindById().getCount());

        assertEquals(first.get(), second.get());

        assertSingleRow(second.get(), topStudentSelectQuery, getPersonPSConsumer(second.get().getNif()), AssertUtils::assertTopStudent, con);
    }

    @Test
    public void testfindAll() {
        topStudentRepo.findAll().join();
        assertEquals(1, topStudentMapperify.getIfindAll().getCount());

        List<TopStudent> second = topStudentRepo.findAll().join();
        assertEquals(2, topStudentMapperify.getIfindAll().getCount());

        assertMultipleRows(con, second, topStudentSelectQuery.substring(0, topStudentSelectQuery.length()-16), AssertUtils::assertTopStudent, second.size());
    }

    //-----------------------------------Create-----------------------------------//

    @Test
    public void testcreate() {
        //Arrange
        TopStudent topStudent = new TopStudent(456, "Manel", new Date(2020, 12, 1), 0, 1, 20, 2016, 0, 0);

        //Act
        Optional<Throwable> success = topStudentRepo.create(topStudent).join();

        //Assert
        assertTrue(!success.isPresent());

        Optional<TopStudent> first = topStudentRepo.findById(456).join();
        assertEquals(0, topStudentMapperify.getIfindById().getCount());

        assertEquals(first.get(), topStudent);
    }

    @Test
    public void testcreateAll() {
        //Arrange
        List<TopStudent> list = new ArrayList<>(2);
        list.add(new TopStudent(456, "Manel", new Date(2020, 12, 1), 0, 1, 20, 2016, 0, 0));
        list.add(new TopStudent(457, "Maria", null, 0, 2, 18, 2010, 0, 0));

        //Act
        Optional<Throwable> success = topStudentRepo.createAll(list).join();

        //Assert
        assertTrue(!success.isPresent());

        Optional<TopStudent> first = topStudentRepo.findById(456).join();
        assertEquals(0, topStudentMapperify.getIfindById().getCount());
        Optional<TopStudent> second = topStudentRepo.findById(457).join();
        assertEquals(0, topStudentMapperify.getIfindById().getCount());

        assertEquals(first.get(), list.get(0));
        assertEquals(second.get(), list.get(1));
    }

    //-----------------------------------Update-----------------------------------//
    @Test
    public void testupdate() throws SQLException {
        ResultSet rs = executeQuery("select CAST(P.version as bigint), CAST(S2.version as bigint), CAST(TS.version as bigint) version from Person P " +
                "inner join Student S2 on P.nif = S2.nif " +
                "inner join TopStudent TS on S2.nif = TS.nif where P.nif = ?", getPersonPSConsumer(454), con);
        TopStudent topStudent = new TopStudent(454, "Carlos", new Date(2010, 6, 3), rs.getLong(2),
                4, 6, 7, rs.getLong(3), rs.getLong(1));

        Optional<Throwable> success = topStudentRepo.update(topStudent).join();

        assertTrue(!success.isPresent());

        Optional<TopStudent> first = topStudentRepo.findById(454).join();
        assertEquals(1, topStudentMapperify.getIfindById().getCount());
        topStudentRepo.findById(454).join();
        assertEquals(1, topStudentMapperify.getIfindById().getCount());

        assertEquals(first.get(), topStudent);
    }

    @Test
    public void testupdateAll() throws SQLException {
        List<Person> list = new ArrayList<>(2);
        ResultSet rs = executeQuery("select CAST(version as bigint) version from Person where nif = ?", getPersonPSConsumer(321), con);
        list.add(new Person(321, "Maria", new Date(2010, 2, 3), rs.getLong(1)));

        rs = executeQuery("select CAST(version as bigint) version from Person where nif = ?", getPersonPSConsumer(454), con);
        list.add(new Person(454, "Ze Miguens", new Date(1080, 2, 4), rs.getLong(1)));

        Optional<Throwable> success = personRepo.updateAll(list).join();

        assertTrue(!success.isPresent());

        Optional<Person> first = personRepo.findById(321).join();
        assertEquals(2, personMapperify.getIfindById().getCount());
        Optional<Person> second = personRepo.findById(454).join();
        assertEquals(2, personMapperify.getIfindById().getCount());

        assertEquals(first.get(), list.get(0));
        assertEquals(second.get(), list.get(1));
    }

    @Test
    public void testSameReferenceUpdate() throws SQLException {
        ResultSet rs = executeQuery(employeeSelectQuery, getEmployeePSConsumer("Bob"), con);

        CompletableFuture<Company> companyRepoById = companyRepo.findById(new Company.PrimaryKey(rs.getInt("companyId"), rs.getInt("companyCid")))
                .thenApply(company -> company.orElseThrow(() -> new DataMapperException("Company not found")));

        Employee employee = new Employee(rs.getInt("id"),"Boba", rs.getLong("version"), companyRepoById);

        assertTrue(!employeeRepo.update(employee).join().isPresent());

        assertSingleRow(employee, employeeSelectQuery, getEmployeePSConsumer("Boba"), AssertUtils::assertEmployeeWithExternals, con);
        Optional<Employee> first = companyRepoById
                .join()
                .getEmployees()
                .join()
                .stream()
                .filter(employee1 -> employee1.getIdentityKey().equals(employee.getIdentityKey()))
                .findFirst();
        assertTrue(first.isPresent());
    }

    @Test
    public void testRemoveReferenceUpdate() throws SQLException {
        ResultSet rs = executeQuery(employeeSelectQuery, getEmployeePSConsumer("Bob"), con);
        Employee employee = new Employee(rs.getInt("id"),"Boba", rs.getLong("version"), null);

        Company company = companyRepo.findById(new Company.PrimaryKey(1, 1)).join().orElseThrow(() -> new DataMapperException("Company not found"));

        assertTrue(!employeeRepo.update(employee).join().isPresent());

        assertSingleRow(employee, employeeSelectQuery, getEmployeePSConsumer("Boba"), AssertUtils::assertEmployeeWithExternals, con);
        Optional<Employee> first = company
                .getEmployees()
                .join()
                .stream()
                .filter(employee1 -> employee1.getIdentityKey().equals(employee.getIdentityKey()))
                .findFirst();
        assertTrue(!first.isPresent());
    }

    @Test
    public void testRemoveNNReferenceUpdate() throws SQLException {
        ResultSet rs = executeQuery(bookSelectQuery, getBookPSConsumer("1001 noites"), con);

        Author author = authorRepo.findWhere(new Pair<>("name", "Ze")).join().get(0);

        assertTrue(author.getBooks().join().stream().anyMatch(book -> {
            try {
                return book.getIdentityKey().equals(rs.getLong("id"));
            } catch (SQLException e) {
                throw new DataMapperException(e);
            }
        }));

        Book book = new Book(rs.getInt("id"), rs.getString("name"), rs.getLong("version"), null);

        assertTrue(!bookRepo.update(book).join().isPresent());

        assertEquals(0, authorMapperify.getIfindById().getCount());

        assertSingleRow(book, bookSelectQuery, getBookPSConsumer(rs.getString("name")), (book1, resultSet) -> AssertUtils.assertBook(book1, resultSet, con), con);
        Optional<Book> first = author
                .getBooks()
                .join()
                .stream()
                .filter(book1 -> book1.getIdentityKey().equals(book.getIdentityKey()))
                .findFirst();
        assertTrue(!first.isPresent());
    }

    @Test
    public void testUpdateReferenceUpdate() throws SQLException {
        ResultSet rs = executeQuery(employeeSelectQuery, getEmployeePSConsumer("Bob"), con);

        Company company = companyRepo.findById(new Company.PrimaryKey(1, 1))
                .join()
                .orElseThrow(() -> new DataMapperException("Company not found"));

        CompletableFuture<Company> company1 = companyRepo.findById(new Company.PrimaryKey(1, 2))
                .thenApply(optionalCompany -> optionalCompany.orElseThrow(() -> new DataMapperException("Company not found")));

        Employee employee = new Employee(rs.getInt("id"),"Boba", rs.getLong("version"), company1);

        assertTrue(!employeeRepo.update(employee).join().isPresent());

        assertSingleRow(employee, employeeSelectQuery, getEmployeePSConsumer("Boba"), AssertUtils::assertEmployeeWithExternals, con);
        Optional<Employee> first = company
                .getEmployees()
                .join()
                .stream()
                .filter(employee1 -> employee1.getIdentityKey().equals(employee.getIdentityKey()))
                .findFirst();
        assertTrue(!first.isPresent());

        first = company1
                .join()
                .getEmployees()
                .join()
                .stream()
                .filter(employee1 -> employee1.getIdentityKey().equals(employee.getIdentityKey()))
                .findFirst();
        assertTrue(first.isPresent());
    }

    //-----------------------------------DeleteById-----------------------------------//
    @Test
    public void testdeleteById() {
        boolean success = !topStudentRepo.deleteById(454).join().isPresent();

        assertTrue(success);

        Optional<TopStudent> optionalTopStudent = topStudentRepo.findById(454).join();
        assertEquals(2, topStudentMapperify.getIfindById().getCount());
        assertFalse(optionalTopStudent.isPresent());
        assertNotFound(topStudentSelectQuery, getPersonPSConsumer(454), con);
    }

    @Test
    public void testdelete() {
        TopStudent topStudent = new TopStudent(454, null, null, 0, 0, 0, 0, 0, 0);

        boolean success = !topStudentRepo.delete(topStudent).join().isPresent();

        assertTrue(success);

        Optional<TopStudent> optionalTopStudent = topStudentRepo.findById(454).join();
        assertEquals(1, topStudentMapperify.getIfindById().getCount());
        assertFalse(optionalTopStudent.isPresent());
        assertNotFound(topStudentSelectQuery, getPersonPSConsumer(topStudent.getNif()), con);
    }

    @Test
    public void testdeleteAll() throws SQLException {
        List<Integer> list = new ArrayList<>(2);
        ResultSet rs = executeQuery("select id from Employee where name = ?", getEmployeePSConsumer("Bob"), con);
        list.add(rs.getInt("id"));

        rs = executeQuery("select id from Employee where name = ?", getEmployeePSConsumer("Charles"), con);
        list.add(rs.getInt("id"));

        boolean success = !employeeRepo.deleteAll(list).join().isPresent();

        assertTrue(success);

        assertNotFound(employeeSelectQuery, getEmployeePSConsumer("Bob"), con);
        assertNotFound(employeeSelectQuery, getEmployeePSConsumer("Charles"), con);

        Optional<Employee> optionalPerson = employeeRepo.findById(list.remove(0)).join();
        assertEquals(3, employeeMapperify.getIfindById().getCount());
        assertFalse(optionalPerson.isPresent());

        Optional<Employee> optionalPerson2 = employeeRepo.findById(list.remove(0)).join();
        assertEquals(4, employeeMapperify.getIfindById().getCount());
        assertFalse(optionalPerson2.isPresent());
    }
}