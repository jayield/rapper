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

    private UnitOfWork unit;

    public DataRepositoryTests() throws NoSuchFieldException, IllegalAccessException {
        Field repositoryMapField = MapperRegistry.class.getDeclaredField("repositoryMap");
        repositoryMapField.setAccessible(true);
        repositoryMap = (Map<Class, MapperRegistry.Container>) repositoryMapField.get(null);
    }

    @Before
    public void before() {
        assertEquals(0, UnitOfWork.numberOfOpenConnections.get());
        repositoryMap.clear();

        ConnectionManager manager = ConnectionManager.getConnectionManager(
                "jdbc:hsqldb:file:" + URLDecoder.decode(this.getClass().getClassLoader().getResource("testdb").getPath()) + "/testdb",
                "SA",
                ""
        );

        unit = new UnitOfWork(manager::getConnection);

        SQLConnection con = unit.getConnection().join();
        SQLUtils.<ResultSet>callbackToPromise(ar -> con.call("{call deleteDB()}", ar)).join();
        SQLUtils.<ResultSet>callbackToPromise(ar -> con.call("{call populateDB()}", ar)).join();
        unit.commit().join();

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
    public void after(){
        unit.rollback().join();
        assertEquals(0, UnitOfWork.numberOfOpenConnections.get());
    }

    @Test
    public void testGetNumberOfEntriesWhere(){
        long numberOfEntries = companyRepo.getNumberOfEntries(unit, new Pair<>("id", 1)).join();
        assertEquals(11, numberOfEntries);
    }

    @Test
    public void testGetNumberOfEntries(){
        long numberOfEntries = companyRepo.getNumberOfEntries(unit).join();
        assertEquals(11, numberOfEntries);
    }

    //-----------------------------------Find-----------------------------------//

    @Test
    public void testfindById() {
        Optional<TopStudent> first = topStudentRepo.findById(unit, 454).join();
        assertEquals(1, topStudentMapperify.getIfindById().getCount());

        Optional<TopStudent> second = topStudentRepo.findById(unit, 454).join();
        assertEquals(1, topStudentMapperify.getIfindById().getCount());

        assertEquals(first.get(), second.get());

        SQLConnection con = unit.getConnection().join();
        assertSingleRow(second.get(), topStudentSelectQuery, new JsonArray().add(second.get().getNif()), AssertUtils::assertTopStudent, con);
        unit.rollback().join();
    }

    @Test
    public void testfindAll() {
        topStudentRepo.findAll(unit).join();
        assertEquals(1, topStudentMapperify.getIfindAll().getCount());

        List<TopStudent> second = topStudentRepo.findAll(unit).join();
        assertEquals(2, topStudentMapperify.getIfindAll().getCount());

        SQLConnection con = unit.getConnection().join();
        assertMultipleRows(con, second, topStudentSelectQuery.substring(0, topStudentSelectQuery.length()-16), AssertUtils::assertTopStudent, second.size());
        unit.rollback().join();
    }

    //-----------------------------------Create-----------------------------------//

    @Test
    public void testcreate() {
        //Arrange
        TopStudent topStudent = new TopStudent(456, "Manel", new Date(2020, 12, 1).toInstant(), 0, 1, 20, 2016, 0, 0);

        //Act
        topStudentRepo.create(unit, topStudent)
                .thenCompose(aVoid -> unit.commit())
                .join();

        //Assert
        Optional<TopStudent> first = topStudentRepo.findById(unit, 456).join();
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
        topStudentRepo.createAll(unit, list)
                .thenCompose(aVoid -> unit.commit())
                .join();

        //Assert
        Optional<TopStudent> first = topStudentRepo.findById(unit, 456).join();
        assertEquals(0, topStudentMapperify.getIfindById().getCount());
        Optional<TopStudent> second = topStudentRepo.findById(unit, 457).join();
        assertEquals(0, topStudentMapperify.getIfindById().getCount());

        assertEquals(first.get(), list.get(0));
        assertEquals(second.get(), list.get(1));
    }

    //-----------------------------------Update-----------------------------------//
    @Test
    public void testupdate()  {
        SQLConnection con = unit.getConnection().join();
        ResultSet rs = executeQuery("select CAST(P.version as bigint), CAST(S2.version as bigint), CAST(TS.version as bigint) version from Person P " +
                "inner join Student S2 on P.nif = S2.nif " +
                "inner join TopStudent TS on S2.nif = TS.nif where P.nif = ?", new JsonArray().add(454), con);
        unit.rollback().join();
        JsonArray firstRes = rs.getResults().get(0);
        TopStudent topStudent = new TopStudent(454, "Carlos", new Date(2010, 6, 3).toInstant(), firstRes.getLong(1),
                4, 6, 7, firstRes.getLong(2), firstRes.getLong(0));

        topStudentRepo.update(unit, topStudent)
                .thenCompose(aVoid -> unit.commit())
                .join();

        Optional<TopStudent> first = topStudentRepo.findById(unit, 454).join();
        assertEquals(1, topStudentMapperify.getIfindById().getCount());
        topStudentRepo.findById(unit, 454).join();
        assertEquals(1, topStudentMapperify.getIfindById().getCount());

        assertEquals(first.get(), topStudent);
    }

    @Test
    public void testupdateAll() {
        SQLConnection con = unit.getConnection().join();
        List<Person> list = new ArrayList<>(2);
        ResultSet rs = executeQuery("select CAST(version as bigint) version from Person where nif = ?", new JsonArray().add(321), con);
        list.add(new Person(321, "Maria", new Date(2010, 2, 3).toInstant(), rs.getResults().get(0).getLong(0)));

        rs = executeQuery("select CAST(version as bigint) version from Person where nif = ?", new JsonArray().add(454), con);
        list.add(new Person(454, "Ze Miguens", new Date(1080, 2, 4).toInstant(), rs.getResults().get(0).getLong(0)));
        unit.rollback().join();

        personRepo.updateAll(unit, list)
                .thenCompose(aVoid -> unit.commit())
                .join();

        Optional<Person> first = personRepo.findById(unit, 321).join();
        assertEquals(2, personMapperify.getIfindById().getCount());
        Optional<Person> second = personRepo.findById(unit, 454).join();
        assertEquals(2, personMapperify.getIfindById().getCount());

        assertEquals(first.get(), list.get(0));
        assertEquals(second.get(), list.get(1));
    }

    //-----------------------------------DeleteById-----------------------------------//
    @Test
    public void testdeleteById() {
        topStudentRepo.deleteById(unit, 454)
                .thenCompose(aVoid -> unit.commit())
                .join();

        Optional<TopStudent> optionalTopStudent = topStudentRepo.findById(unit, 454).join();
        assertEquals(2, topStudentMapperify.getIfindById().getCount());
        assertFalse(optionalTopStudent.isPresent());
        SQLConnection con = unit.getConnection().join();
        assertNotFound(topStudentSelectQuery, new JsonArray().add(454), con);
        unit.rollback().join();
    }

    @Test
    public void testdelete() {
        TopStudent topStudent = new TopStudent(454, null, null, 0, 0, 0, 0, 0, 0);

        topStudentRepo.delete(unit, topStudent)
                .thenCompose(aVoid -> unit.commit())
                .join();

        Optional<TopStudent> optionalTopStudent = topStudentRepo.findById(unit, 454).join();
        assertEquals(1, topStudentMapperify.getIfindById().getCount());
        assertFalse(optionalTopStudent.isPresent());
        SQLConnection con = unit.getConnection().join();
        assertNotFound(topStudentSelectQuery, new JsonArray().add(topStudent.getNif()), con);
        unit.rollback().join();
    }

    @Test
    public void testdeleteAll() {
        SQLConnection con = unit.getConnection().join();
        List<Integer> list = new ArrayList<>(2);
        ResultSet rs = executeQuery("select id from Employee where name = ?", new JsonArray().add("Bob"), con);
        JsonObject firstRes = rs.getRows(true).get(0);
        list.add(firstRes.getInteger("id"));

        rs = executeQuery("select id from Employee where name = ?", new JsonArray().add("Charles"), con);
        firstRes = rs.getRows(true).get(0);
        list.add(firstRes.getInteger("id"));

        employeeRepo.deleteAll(unit, list)
                .thenCompose(aVoid -> unit.commit())
                .join();

        con = unit.getConnection().join();
        assertNotFound(employeeSelectQuery, new JsonArray().add("Bob"), con);
        assertNotFound(employeeSelectQuery, new JsonArray().add("Charles"), con);

        Optional<Employee> optionalPerson = employeeRepo.findById(unit, list.remove(0)).join();
        assertTrue(3 >= employeeMapperify.getIfindById().getCount());
        assertFalse(optionalPerson.isPresent());

        Optional<Employee> optionalPerson2 = employeeRepo.findById(unit, list.remove(0)).join();
        assertTrue(4 >= employeeMapperify.getIfindById().getCount());
        assertFalse(optionalPerson2.isPresent());
        unit.rollback().join();
    }
}