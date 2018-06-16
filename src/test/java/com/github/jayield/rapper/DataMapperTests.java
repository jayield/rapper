package com.github.jayield.rapper;

import com.github.jayield.rapper.domainModel.*;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.utils.*;
import com.mchange.v2.sql.SqlUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import org.junit.Before;
import org.junit.Test;

import java.net.URLDecoder;
import java.sql.Connection;
import java.util.Date;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.github.jayield.rapper.AssertUtils.*;
import static com.github.jayield.rapper.TestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DataMapperTests {
    private static String detailMessage = "DomainObject wasn't found";

    private final DataMapper<Person, Integer> personMapper = (DataMapper<Person, Integer>) MapperRegistry.getRepository(Person.class).getMapper();
    private final DataMapper<Car, CarKey> carMapper = (DataMapper<Car, CarKey>) MapperRegistry.getRepository(Car.class).getMapper();
    private final DataMapper<TopStudent, Integer> topStudentMapper = (DataMapper<TopStudent, Integer>) MapperRegistry.getRepository(TopStudent.class).getMapper();
    private final DataMapper<Company, Company.PrimaryKey> companyMapper = (DataMapper<Company, Company.PrimaryKey>) MapperRegistry.getRepository(Company.class).getMapper();
    private final DataMapper<Book, Long> bookMapper = (DataMapper<Book, Long>) MapperRegistry.getRepository(Book.class).getMapper();
    private final DataMapper<Employee, Integer> employeeMapper = (DataMapper<Employee, Integer>) MapperRegistry.getRepository(Employee.class).getMapper();
    private final DataMapper<Dog, Dog.DogPK> dogMapper = (DataMapper<Dog, Dog.DogPK>) MapperRegistry.getRepository(Dog.class).getMapper();

    @Before
    public void start() {
        ConnectionManager manager = ConnectionManager.getConnectionManager(
                "jdbc:hsqldb:file:" + URLDecoder.decode(this.getClass().getClassLoader().getResource("testdb").getPath()) + "/testdb",
                "SA", "");
        Supplier<CompletableFuture<SQLConnection>> connectionSupplier = manager::getConnection;
        UnitOfWork.removeCurrent();

        CompletableFuture<SQLConnection> con = connectionSupplier.get();
        con.thenCompose(sqlConnection ->
                SQLUtils.<ResultSet>callbackToPromise(ar -> sqlConnection.call("{call deleteDB()}", ar))
                .thenAccept(v -> SQLUtils.<ResultSet>callbackToPromise(ar -> sqlConnection.call("{call populateDB()}", ar)))
                .thenAccept(v -> SQLUtils.callbackToPromise(sqlConnection::commit))
                .thenAccept(v -> sqlConnection.close())
        ).join();
        UnitOfWork.newCurrent(connectionSupplier);
    }

    @Test
    public void testGetNumberOfEntriesWhere(){
        long numberOfEntries = companyMapper.getNumberOfEntries(new Pair<>("id", 1)).join();
        assertEquals(11, numberOfEntries);
    }

    @Test
    public void testGetNumberOfEntries(){
        long numberOfEntries = companyMapper.getNumberOfEntries().join();
        assertEquals(11, numberOfEntries);
    }

    //-----------------------------------FindWhere-----------------------------------//
    @Test
    public void testSimpleFindWhere() {
        List<Person> people = personMapper.findWhere(new Pair<>("name", "Jose")).join();

        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        SQLUtils.<ResultSet>callbackToPromise(ar ->
                con.queryWithParams("select nif, name, birthday, CAST(version as bigint) version from Person where name = ?",
                        new JsonArray().add("Jose"), ar))
                .thenAccept(resultSet -> {
                    if(resultSet.getRows(true).isEmpty())
                        fail("Database has no data");
                    assertPerson(people.remove(0), resultSet.getRows(true).get(0));
                }).join();
    }

    @Test
    public void testEmbeddedIdFindWhere() {
        List<Car> cars = carMapper.findWhere(new Pair<>("brand", "Mitsubishi")).join();

        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();

        SQLUtils.<ResultSet>callbackToPromise(ar ->
                con.queryWithParams("select owner, plate, brand, model, CAST(version as bigint) version from Car where brand = ?",
                        new JsonArray().add("Mitsubishi"), ar))
                .thenAccept(resultSet -> {
                    if(resultSet.getRows(true).isEmpty())
                        fail("Database has no data");
                    assertCar(cars.remove(0), resultSet.getRows(true).get(0));
                })
                .thenAccept(v -> SQLUtils.callbackToPromise(con::rollback))
                .thenAccept(v -> con.close()).join();
    }

    @Test
    public void testNNExternalFindWhere(){
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        Book book = bookMapper.findWhere(new Pair<>("name", "1001 noites")).join().get(0);
        assertSingleRow(book, bookSelectQuery, new JsonArray().add(book.getName()), (book1, rs) -> AssertUtils.assertBook(book1, rs, con ), con);
    }

    @Test
    public void testParentExternalFindWhere(){
        Employee employee = employeeMapper.findWhere(new Pair<>("name", "Bob")).join().get(0);
        assertSingleRow(employee, employeeSelectQuery, new JsonArray().add("Bob"), AssertUtils::assertEmployee, UnitOfWork.getCurrent().getConnection().join());
    }

    @Test
    public void testPaginationFindWhere() {
        List<Company> companies = companyMapper.findWhere(0, 10, new Pair<>("id", 1)).join();
        assertMultipleRows(UnitOfWork.getCurrent().getConnection().join(), companies, companySelectTop10Query, AssertUtils::assertCompany, 10);
    }

    @Test
    public void testSecondPageFindWhere() {
        List<Company> companies = companyMapper.findWhere(1, 10, new Pair<>("id", 1)).join();
        assertEquals(1, companies.size());
        assertSingleRow(companies.get(0), companySelectQuery, new JsonArray().add(1).add(11), AssertUtils::assertCompany, UnitOfWork.getCurrent().getConnection().join());
    }

    //-----------------------------------FindById-----------------------------------//
    @Test
    public void testSimpleFindById(){
        int nif = 321;
        Person person = personMapper
                .findById(nif)
                .join()
                .orElseThrow(() -> new AssertionError(detailMessage));
        assertSingleRow(person, personSelectQuery, new JsonArray().add(nif), AssertUtils::assertPerson, UnitOfWork.getCurrent().getConnection().join());
    }

    @Test
    public void testEmbeddedIdFindById() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        int owner = 2; String plate = "23we45";
        Car car = carMapper
                .findById(new CarKey(owner, plate))
                .join()
                .orElseThrow(() -> new AssertionError(detailMessage));
        assertSingleRow(car, carSelectQuery, new JsonArray().add(owner).add(plate), AssertUtils::assertCar, con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    @Test
    public void testHierarchyFindById() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        TopStudent topStudent = topStudentMapper
                .findById(454)
                .join()
                .orElseThrow(() -> new AssertionError(detailMessage));
        assertSingleRow(topStudent, topStudentSelectQuery, new JsonArray().add(454), AssertUtils::assertTopStudent, con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    @Test
    public void testEmbeddedIdExternalFindById(){
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        int companyId = 1, companyCid = 1;
        Company company = companyMapper
                .findById(new Company.PrimaryKey(companyId, companyCid))
                .join()
                .orElseThrow(() -> new AssertionError(detailMessage));
        assertSingleRow(company, "select id, cid, motto, CAST(version as bigint) Cversion from Company where id = ? and cid = ?",
                new JsonArray().add(companyId).add(companyCid), AssertUtils::assertCompany, con);
    }

    //-----------------------------------FindAll-----------------------------------//
    @Test
    public void testSimpleFindAll(){
        List<Person> people = personMapper.findAll().join();
        assertMultipleRows(UnitOfWork.getCurrent().getConnection().join(), people, "select nif, name, birthday, CAST(version as bigint) version from Person", AssertUtils::assertPerson, 2);
    }

    @Test
    public void testEmbeddedIdFindAll() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        List<Car> cars = carMapper.findAll().join();
        assertMultipleRows(con, cars, "select owner, plate, brand, model, CAST(version as bigint) version from Car", AssertUtils::assertCar, 1);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    @Test
    public void testHierarchyFindAll(){
        List<TopStudent> topStudents = topStudentMapper.findAll().join();
        assertMultipleRows(UnitOfWork.getCurrent().getConnection().join(), topStudents, topStudentSelectQuery.substring(0, topStudentSelectQuery.length() - 15), AssertUtils::assertTopStudent, 1);
    }

    @Test
    public void testPaginationFindAll() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        List<Company> companies = companyMapper.findAll(0, 10).join();
        assertMultipleRows(con, companies, companySelectTop10Query, AssertUtils::assertCompany, 10);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    //-----------------------------------Create-----------------------------------//
    @Test
    public void testSimpleCreate() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        Person person = new Person(123, "abc", new Date(1969, 6, 9).toInstant(), 0);
        personMapper.create(person)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertSingleRow(person, personSelectQuery, new JsonArray().add(person.getNif()), AssertUtils::assertPerson, con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    @Test
    public void testEmbeddedIdCreate() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        Car car = new Car(1, "58en60", "Mercedes", "ES1", 0);
        carMapper.create(car)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertSingleRow(car, carSelectQuery, new JsonArray().add(car.getIdentityKey().getOwner()).add(car.getIdentityKey().getPlate()), AssertUtils::assertCar, con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    @Test
    public void testHierarchyCreate() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        TopStudent topStudent = new TopStudent(456, "Manel", new Date(2020, 12, 1).toInstant(), 0, 1, 20, 2016, 0, 0);
        topStudentMapper.create(topStudent)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertSingleRow(topStudent, topStudentSelectQuery, new JsonArray().add(topStudent.getNif()), AssertUtils::assertTopStudent, con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    @Test
    public void testSingleExternalCreate() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        CompletableFuture<Company> companyCompletableFuture = companyMapper
                .findById(new Company.PrimaryKey(1, 1))
                .thenApply(company -> company.orElseThrow(() -> new DataMapperException(("Company not found"))));

        Employee employee = new Employee(0, "Hugo", 0, companyCompletableFuture);
        employeeMapper.create(employee)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertSingleRow(employee, employeeSelectQuery, new JsonArray().add("Hugo"), AssertUtils::assertEmployee, con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    @Test
    public void testSingleExternalNullCreate() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        Employee employee = new Employee(0, "Hugo", 0, null);
        employeeMapper.create(employee)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertSingleRow(employee, employeeSelectQuery, new JsonArray().add("Hugo"), AssertUtils::assertEmployee, con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    @Test
    public void testNoVersionCreate() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        Dog dog = new Dog(new Dog.DogPK("Bobby", "Pitbull"), 5);
        dogMapper.create(dog)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertSingleRow(dog, dogSelectQuery, new JsonArray().add("Bobby").add("Pitbull"), AssertUtils::assertDog, con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    //-----------------------------------Update-----------------------------------//
    @Test
    public void testSimpleUpdate() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        ResultSet rs = executeQuery("select CAST(version as bigint) version from Person where nif = ?", new JsonArray().add(321), con);
        Person person = new Person(321, "Maria", new Date(2010, 2, 3).toInstant(), rs.getResults().get(0).getLong(0));

        personMapper.update(person)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();

        assertSingleRow(person, personSelectQuery, new JsonArray().add(person.getNif()), AssertUtils::assertPerson, con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    @Test
    public void testEmbeddedIdUpdate() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        ResultSet rs = executeQuery("select CAST(version as bigint) version from Car where owner = ? and plate = ?", new JsonArray().add(2).add( "23we45"), con);
        Car car = new Car(2, "23we45", "Mitsubishi", "lancer evolution", rs.getResults().get(0).getLong(0));

        carMapper.update(car)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();

        assertSingleRow(car, carSelectQuery, new JsonArray().add(car.getIdentityKey().getOwner()).add(car.getIdentityKey().getPlate()), AssertUtils::assertCar, con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    @Test
    public void testHierarchyUpdate() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        ResultSet rs = executeQuery("select CAST(P.version as bigint), CAST(S2.version as bigint), CAST(TS.version as bigint) version from Person P " +
                "inner join Student S2 on P.nif = S2.nif " +
                "inner join TopStudent TS on S2.nif = TS.nif where P.nif = ?", new JsonArray().add(454), con);
        JsonArray first = rs.getResults().get(0);
        TopStudent topStudent = new TopStudent(454, "Carlos", new Date(2010, 6, 3).toInstant(), first.getLong(1),
                4, 6, 7, first.getLong(2), first.getLong(0));

        topStudentMapper.update(topStudent)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();

        assertSingleRow(topStudent, topStudentSelectQuery, new JsonArray().add(topStudent.getNif()), AssertUtils::assertTopStudent, con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    @Test
    public void testSingleExternalUpdate() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        ResultSet rs = executeQuery(employeeSelectQuery, new JsonArray().add("Bob"), con);
        JsonObject first = rs.getRows(true).get(0);
        Employee employee = new Employee(first.getInteger("id"),"Boba", first.getLong("version"),
                companyMapper
                        .findById(new Company.PrimaryKey(1, 2))
                        .thenApply(company -> company.orElseThrow(() -> new DataMapperException(("Company not found")))));

        employeeMapper.update(employee)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();

        assertSingleRow(employee, employeeSelectQuery, new JsonArray().add("Boba"), AssertUtils::assertEmployee, con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    @Test
    public void testSingleExternalNullUpdate() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        ResultSet rs = executeQuery(employeeSelectQuery, new JsonArray().add("Bob"), con);
        JsonObject first = rs.getRows(true).get(0);
        Employee employee = new Employee(
                first.getInteger("id"),"Boba",
                first.getLong("version"), null);

        employeeMapper.update(employee)
                .exceptionally(throwable -> {
                    fail();
                    return null;
                })
                .join();

        assertSingleRow(employee, employeeSelectQuery, new JsonArray().add("Boba"), AssertUtils::assertEmployee, con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    @Test
    public void testNoVersionUpdate() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        ResultSet rs = executeQuery(dogSelectQuery, new JsonArray().add("Doggy").add("Bulldog"), con);
        JsonObject first = rs.getRows(true).get(0);
        Dog dog = new Dog(
                new Dog.DogPK(
                        first.getString("name"),
                        first.getString("race")), 6);
        dogMapper.update(dog)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertSingleRow(dog, dogSelectQuery, new JsonArray().add("Doggy").add("Bulldog"), AssertUtils::assertDog, con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    //-----------------------------------DeleteById-----------------------------------//
    @Test
    public void testSimpleDeleteById() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        personMapper.deleteById(321)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertNotFound(personSelectQuery, new JsonArray().add(321), con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    @Test
    public void testEmbeddedDeleteById() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        carMapper.deleteById(new CarKey(2, "23we45"))
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertNotFound(carSelectQuery, new JsonArray().add(2).add("23we45"), con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    @Test
    public void testHierarchyDeleteById() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        topStudentMapper.deleteById(454)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertNotFound(topStudentSelectQuery, new JsonArray().add(454), con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    @Test
    public void testSingleExternalDeleteById() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        ResultSet rs = executeQuery(employeeSelectQuery, new JsonArray().add("Bob"), con);
        employeeMapper.deleteById(rs.getRows(true).get(0).getInteger("id"))
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertNotFound(employeeSelectQuery, new JsonArray().add("Bob"), con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    //-----------------------------------Delete-----------------------------------//
    @Test
    public void testSimpleDelete() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        Person person = new Person(321, null, null, 0);
        personMapper.delete(person)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertNotFound(personSelectQuery, new JsonArray().add(321), con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    @Test
    public void testEmbeddedDelete() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        Car car = new Car(2, "23we45", null, null, 0);
        carMapper.delete(car)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertNotFound(carSelectQuery, new JsonArray().add(2).add( "23we45"), con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    @Test
    public void testHierarchyDelete() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        TopStudent topStudent = new TopStudent(454, null, null, 0, 0, 0, 0, 0, 0);
        topStudentMapper.delete(topStudent)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertNotFound(topStudentSelectQuery, new JsonArray().add(454), con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }

    @Test
    public void testSingleExternalDelete() {
        SQLConnection con = UnitOfWork.getCurrent().getConnection().join();
        ResultSet rs = executeQuery(employeeSelectQuery, new JsonArray().add("Bob"), con);
        JsonObject first = rs.getRows(true).get(0);
        Employee employee = new Employee(first.getInteger("id"), first.getString("name"), first.getLong("version"),
                companyMapper
                        .findById(new Company.PrimaryKey(1, 2))
                        .thenApply(company -> company.orElseThrow(() -> new DataMapperException(("Company not found")))));
        employeeMapper.delete(employee)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertNotFound(employeeSelectQuery, new JsonArray().add("Bob"), con);
        SQLUtils.callbackToPromise(con::rollback).thenAccept(v -> con.close());
    }
}
