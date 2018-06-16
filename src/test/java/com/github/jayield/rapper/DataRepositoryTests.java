package com.github.jayield.rapper;

import com.github.jayield.rapper.domainModel.*;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.exceptions.UnitOfWorkException;
import com.github.jayield.rapper.utils.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.URLDecoder;
import java.util.Date;
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

    private SQLConnection con;
    public DataRepositoryTests() throws NoSuchFieldException, IllegalAccessException {
        Field repositoryMapField = MapperRegistry.class.getDeclaredField("repositoryMap");
        repositoryMapField.setAccessible(true);
        repositoryMap = (Map<Class, MapperRegistry.Container>) repositoryMapField.get(null);
    }

    @Before
    public void before() {
        UnitOfWork.removeCurrent();
        repositoryMap.clear();

        ConnectionManager manager = ConnectionManager.getConnectionManager(
                "jdbc:hsqldb:file:"+URLDecoder.decode(this.getClass().getClassLoader().getResource("testdb").getPath())+"/testdb",
                "SA", "");
        con = manager.getConnection().join();
        SQLUtils.<ResultSet>callbackToPromise(ar -> con.call("{call deleteDB()}", ar)).join();
        SQLUtils.<ResultSet>callbackToPromise(ar -> con.call("{call populateDB()}", ar)).join();
        SQLUtils.callbackToPromise(con::commit).join();

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

    @Test
    public void testGetNumberOfEntriesWhere(){
        long numberOfEntries = companyRepo.getNumberOfEntries(new Pair<>("id", 1)).join();
        assertEquals(11, numberOfEntries);
    }

    @Test
    public void testGetNumberOfEntries(){
        long numberOfEntries = companyRepo.getNumberOfEntries().join();
        assertEquals(11, numberOfEntries);
    }

    //-----------------------------------Find-----------------------------------//

    @Test
    public void testfindById() {
        Optional<TopStudent> first = topStudentRepo.findById(454).join();
        assertEquals(1, topStudentMapperify.getIfindById().getCount());

        Optional<TopStudent> second = topStudentRepo.findById(454).join();
        assertEquals(1, topStudentMapperify.getIfindById().getCount());

        assertEquals(first.get(), second.get());

        assertSingleRow(second.get(), topStudentSelectQuery, new JsonArray().add(second.get().getNif()), AssertUtils::assertTopStudent, con);
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
        TopStudent topStudent = new TopStudent(456, "Manel", new Date(2020, 12, 1).toInstant(), 0, 1, 20, 2016, 0, 0);

        //Act
        topStudentRepo.create(topStudent)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();

        //Assert
        Optional<TopStudent> first = topStudentRepo.findById(456).join();
        assertEquals(0, topStudentMapperify.getIfindById().getCount());

        assertEquals(first.get(), topStudent);
    }

    @Test
    public void testcreateAll() {
        //Arrange
        List<TopStudent> list = new ArrayList<>(2);
        list.add(new TopStudent(456, "Manel", new Date(2020, 12, 1).toInstant(), 0, 1, 20, 2016, 0, 0));
        list.add(new TopStudent(457, "Maria", null, 0, 2, 18, 2010, 0, 0));

        //Act
        topStudentRepo.createAll(list)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();

        //Assert
        Optional<TopStudent> first = topStudentRepo.findById(456).join();
        assertEquals(0, topStudentMapperify.getIfindById().getCount());
        Optional<TopStudent> second = topStudentRepo.findById(457).join();
        assertEquals(0, topStudentMapperify.getIfindById().getCount());

        assertEquals(first.get(), list.get(0));
        assertEquals(second.get(), list.get(1));
    }

    //-----------------------------------Update-----------------------------------//
    @Test
    public void testupdate()  {
        ResultSet rs = executeQuery("select CAST(P.version as bigint), CAST(S2.version as bigint), CAST(TS.version as bigint) version from Person P " +
                "inner join Student S2 on P.nif = S2.nif " +
                "inner join TopStudent TS on S2.nif = TS.nif where P.nif = ?", new JsonArray().add(454), con);
        JsonArray firstRes = rs.getResults().get(0);
        TopStudent topStudent = new TopStudent(454, "Carlos", new Date(2010, 6, 3).toInstant(), firstRes.getLong(1),
                4, 6, 7, firstRes.getLong(2), firstRes.getLong(0));

        topStudentRepo.update(topStudent)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();

        Optional<TopStudent> first = topStudentRepo.findById(454).join();
        assertEquals(1, topStudentMapperify.getIfindById().getCount());
        topStudentRepo.findById(454).join();
        assertEquals(1, topStudentMapperify.getIfindById().getCount());

        assertEquals(first.get(), topStudent);
    }

    @Test
    public void testupdateAll() {
        List<Person> list = new ArrayList<>(2);
        ResultSet rs = executeQuery("select CAST(version as bigint) version from Person where nif = ?", new JsonArray().add(321), con);
        list.add(new Person(321, "Maria", new Date(2010, 2, 3).toInstant(), rs.getResults().get(0).getLong(0)));

        rs = executeQuery("select CAST(version as bigint) version from Person where nif = ?", new JsonArray().add(454), con);
        list.add(new Person(454, "Ze Miguens", new Date(1080, 2, 4).toInstant(), rs.getResults().get(0).getLong(0)));

        personRepo.updateAll(list)
                .exceptionally(throwable -> {
                    fail();
                    return null;
                })
                .join();

        Optional<Person> first = personRepo.findById(321).join();
        assertEquals(2, personMapperify.getIfindById().getCount());
        Optional<Person> second = personRepo.findById(454).join();
        assertEquals(2, personMapperify.getIfindById().getCount());

        assertEquals(first.get(), list.get(0));
        assertEquals(second.get(), list.get(1));
    }

    @Test
    public void testSameReferenceUpdate() {
        ResultSet rs = executeQuery(employeeSelectQuery, new JsonArray().add("Bob"), con);
        JsonObject firstRes = rs.getRows().get(0);
        CompletableFuture<Company> companyRepoById = companyRepo.findById(new Company.PrimaryKey(firstRes.getInteger("companyId"), firstRes.getInteger("companyCid")))
                .thenApply(company -> company.orElseThrow(() -> new DataMapperException("Company not found")));

        Employee employee = new Employee(firstRes.getInteger("id"),"Boba", firstRes.getLong("version"), companyRepoById);

        employeeRepo.update(employee)
                .exceptionally(throwable -> {
                    fail();
                    return null;
                })
                .join();

        assertSingleRow(employee, employeeSelectQuery, new JsonArray().add("Boba"), AssertUtils::assertEmployeeWithExternals, con);
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
    public void testRemoveReferenceUpdate() {
        ResultSet rs = executeQuery(employeeSelectQuery, new JsonArray("Bob"), con);
        JsonObject firstRes = rs.getRows().get(0);
        Employee employee = new Employee(firstRes.getInteger("id"),"Boba", firstRes.getLong("version"), null);

        Company company = companyRepo.findById(new Company.PrimaryKey(1, 1)).join().orElseThrow(() -> new DataMapperException("Company not found"));

        employeeRepo.update(employee)
                .exceptionally(throwable -> {
                    fail();
                    return null;
                })
                .join();

        assertSingleRow(employee, employeeSelectQuery, new JsonArray().add("Boba"), AssertUtils::assertEmployeeWithExternals, con);
        Optional<Employee> first = company
                .getEmployees()
                .join()
                .stream()
                .filter(employee1 -> employee1.getIdentityKey().equals(employee.getIdentityKey()))
                .findFirst();
        assertTrue(!first.isPresent());
    }

    @Test
    public void testRemoveNNReferenceUpdate() {
        ResultSet rs = executeQuery(bookSelectQuery, new JsonArray().add("1001 noites"), con);

        Author author = authorRepo.findWhere(new Pair<>("name", "Ze")).join().get(0);
        JsonObject firstRes = rs.getRows().get(0);
        assertTrue(author.getBooks().join().stream().anyMatch(book ->
                book.getIdentityKey().equals(firstRes.getLong("id"))));

        Book book = new Book(firstRes.getInteger("id"), firstRes.getString("name"), firstRes.getLong("version"), null);

        bookRepo.update(book)
                .exceptionally(throwable -> {
                    fail();
                    return null;
                })
                .join();

        assertEquals(0, authorMapperify.getIfindById().getCount());

        assertSingleRow(book, bookSelectQuery, new JsonArray().add(firstRes.getString("name")), (book1, resultSet) -> AssertUtils.assertBook(book1, resultSet, con), con);
        Optional<Book> first = author
                .getBooks()
                .join()
                .stream()
                .filter(book1 -> book1.getIdentityKey().equals(book.getIdentityKey()))
                .findFirst();
        assertTrue(!first.isPresent());
    }

    @Test
    public void testUpdateReferenceUpdate() {
        ResultSet rs = executeQuery(employeeSelectQuery, new JsonArray().add("Bob"), con);
        JsonObject firstRes = rs.getRows().get(0);
        Company company = companyRepo.findById(new Company.PrimaryKey(1, 1))
                .join()
                .orElseThrow(() -> new DataMapperException("Company not found"));

        boolean failed = false;
        try {
            UnitOfWork.getCurrent();
        } catch (UnitOfWorkException e){
            failed = true;
        }
        assertTrue(failed);

        CompletableFuture<Company> company1 = companyRepo.findById(new Company.PrimaryKey(1, 2))
                .thenApply(optionalCompany -> optionalCompany.orElseThrow(() -> new DataMapperException("Company not found")));

        Employee employee = new Employee(firstRes.getInteger("id"),"Boba", firstRes.getLong("version"), company1);

        failed = false;
        try {
            UnitOfWork.getCurrent();
        } catch (UnitOfWorkException e){
            failed = true;
        }
        assertTrue(failed);

        employeeRepo.update(employee)
                .exceptionally(throwable -> {
                    fail();
                    return null;
                })
                .join();

        assertSingleRow(employee, employeeSelectQuery, new JsonArray().add("Boba"), AssertUtils::assertEmployeeWithExternals, con);
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
        topStudentRepo.deleteById(454)
                .exceptionally(throwable -> {
                    fail();
                    return null;
                })
                .join();

        Optional<TopStudent> optionalTopStudent = topStudentRepo.findById(454).join();
        assertEquals(2, topStudentMapperify.getIfindById().getCount());
        assertFalse(optionalTopStudent.isPresent());
        assertNotFound(topStudentSelectQuery, new JsonArray().add(454), con);
    }

    @Test
    public void testdelete() {
        TopStudent topStudent = new TopStudent(454, null, null, 0, 0, 0, 0, 0, 0);

        topStudentRepo.delete(topStudent)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();

        Optional<TopStudent> optionalTopStudent = topStudentRepo.findById(454).join();
        assertEquals(1, topStudentMapperify.getIfindById().getCount());
        assertFalse(optionalTopStudent.isPresent());
        assertNotFound(topStudentSelectQuery, new JsonArray().add(topStudent.getNif()), con);
    }

    @Test
    public void testdeleteAll() {
        List<Integer> list = new ArrayList<>(2);
        ResultSet rs = executeQuery("select id from Employee where name = ?", new JsonArray().add("Bob"), con);
        JsonObject firstRes = rs.getRows().get(0);
        list.add(firstRes.getInteger("id"));

        rs = executeQuery("select id from Employee where name = ?", new JsonArray().add("Charles"), con);
        firstRes = rs.getRows().get(0);
        list.add(firstRes.getInteger("id"));

        employeeRepo.deleteAll(list)
                .exceptionally(throwable -> {
                    fail();
                    return null;
                })
                .join();

        assertNotFound(employeeSelectQuery, new JsonArray().add("Bob"), con);
        assertNotFound(employeeSelectQuery, new JsonArray().add("Charles"), con);

        Optional<Employee> optionalPerson = employeeRepo.findById(list.remove(0)).join();
        assertTrue(3 >= employeeMapperify.getIfindById().getCount());
        assertFalse(optionalPerson.isPresent());

        Optional<Employee> optionalPerson2 = employeeRepo.findById(list.remove(0)).join();
        assertTrue(4 >= employeeMapperify.getIfindById().getCount());
        assertFalse(optionalPerson2.isPresent());
    }
}