package com.github.jayield.rapper.mapper;

import com.github.jayield.rapper.AssertUtils;
import com.github.jayield.rapper.connections.ConnectionManager;
import com.github.jayield.rapper.domainModel.*;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.mapper.conditions.Condition;
import com.github.jayield.rapper.mapper.conditions.EqualAndCondition;
import com.github.jayield.rapper.mapper.conditions.EqualOrCondition;
import com.github.jayield.rapper.mapper.conditions.OrderCondition;
import com.github.jayield.rapper.mapper.externals.Foreign;
import com.github.jayield.rapper.utils.SqlUtils;
import com.github.jayield.rapper.unitofwork.UnitOfWork;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URLDecoder;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.github.jayield.rapper.AssertUtils.*;
import static com.github.jayield.rapper.TestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DataMapperTests {
    private static String detailMessage = "DomainObject wasn't found";

    private DataMapper<Person, Integer> personMapper;
    private DataMapper<Car, CarKey> carMapper;
    private DataMapper<TopStudent, Integer> topStudentMapper;
    private DataMapper<Company, Company.PrimaryKey> companyMapper;
    private DataMapper<Book, Long> bookMapper;
    private DataMapper<Employee, Integer> employeeMapper;
    private DataMapper<Dog, Dog.DogPK> dogMapper;

    private UnitOfWork unit;

    @Before
    public void start() {
        assertEquals(0, UnitOfWork.getNumberOfOpenConnections().get());
        ConnectionManager manager = ConnectionManager.getConnectionManager(
                "jdbc:hsqldb:file:" + URLDecoder.decode(this.getClass().getClassLoader().getResource("testdb").getPath()) + "/testdb",
                "SA", "");
        Supplier<CompletableFuture<SQLConnection>> connectionSupplier = manager::getConnection;

        unit = new UnitOfWork(connectionSupplier);
        CompletableFuture<SQLConnection> con = unit.getConnection();
        con.thenCompose(sqlConnection ->
                SqlUtils.<ResultSet>callbackToPromise(ar -> sqlConnection.call("{call deleteDB()}", ar))
                .thenAccept(v -> SqlUtils.<ResultSet>callbackToPromise(ar -> sqlConnection.call("{call populateDB()}", ar)))
                //.thenAccept(v -> SQLUtils.callbackToPromise(sqlConnection::commit))
                .thenCompose(v -> unit.commit())
        ).join();

        personMapper =(DataMapper<Person, Integer>) MapperRegistry.getMapper(Person.class, unit);
        carMapper = (DataMapper<Car, CarKey>) MapperRegistry.getMapper(Car.class, unit);
        topStudentMapper = (DataMapper<TopStudent, Integer>) MapperRegistry.getMapper(TopStudent.class, unit);
        companyMapper = (DataMapper<Company, Company.PrimaryKey>) MapperRegistry.getMapper(Company.class, unit);
        bookMapper = (DataMapper<Book, Long>) MapperRegistry.getMapper(Book.class, unit);
        employeeMapper = (DataMapper<Employee, Integer>) MapperRegistry.getMapper(Employee.class, unit);
        dogMapper = (DataMapper<Dog, Dog.DogPK>) MapperRegistry.getMapper(Dog.class, unit);
    }

    @After
    public void after() {
        unit.rollback().join();
        assertEquals(0, UnitOfWork.getNumberOfOpenConnections().get());
    }

    @Test
    public void testGetNumberOfEntriesWhere(){
        long numberOfEntries = companyMapper.getNumberOfEntries(new EqualAndCondition<>("id", 1)).join();
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
        List<Person> people = personMapper.find(new EqualAndCondition<>("name", "Jose")).join();

        SQLConnection con = unit.getConnection().join();
        SqlUtils.<ResultSet>callbackToPromise(ar ->
                con.queryWithParams("select nif, name, birthday, CAST(version as bigint) version from Person where name = ?",
                        new JsonArray().add("Jose"), ar))
                .thenAccept(resultSet -> {
                    if(resultSet.getRows(true).isEmpty())
                        fail("Database has no data");
                    assertPerson(people.remove(0), resultSet.getRows(true).get(0));
                })
                .join();
    }

    @Test
    public void testSimpleFindWhereWithOrAsDelimiter(){
        List<Person> people = personMapper.find(new EqualOrCondition<>("name", "Jose"), new EqualOrCondition<>("name", "Nuno")).join();

        JsonArray jsonArray = new JsonArray();
        jsonArray.add("Jose");
        jsonArray.add("Nuno");

        SQLConnection con = unit.getConnection().join();
        SqlUtils.<ResultSet>callbackToPromise(ar ->
                con.queryWithParams("select nif, name, birthday, CAST(version as bigint) version from Person where name = ? OR name = ?",
                        jsonArray, ar))
                .thenAccept(resultSet -> {
                    if(resultSet.getRows(true).isEmpty())
                        fail("Database has no data");
                    assertPerson(people.get(0), resultSet.getRows(true).get(0));
                    assertPerson(people.get(1), resultSet.getRows(true).get(1));
                })
                .join();
    }

    @Test
    public void testSimpleFindWhereWithComparand() {
        List<Person> people = personMapper.find(new Condition<>("nif", "<", 454)).join();

        SQLConnection con = unit.getConnection().join();
        SqlUtils.<ResultSet>callbackToPromise(ar ->
                con.queryWithParams("select nif, name, birthday, CAST(version as bigint) version from Person where name = ?",
                        new JsonArray().add("Jose"), ar))
                .thenAccept(resultSet -> {
                    if(resultSet.getRows(true).isEmpty())
                        fail("Database has no data");
                    assertPerson(people.remove(0), resultSet.getRows(true).get(0));
                })
                .join();
    }

    @Test
    public void testSimpleFindWhereWithOrderBy() {
        List<Company> companies = companyMapper.find(OrderCondition.desc("cid")).join();

        SQLConnection con = unit.getConnection().join();
        SqlUtils.<ResultSet>callbackToPromise(ar ->
                con.queryWithParams(companySelectQuery, new JsonArray().add("1").add(11), ar))
                .thenAccept(resultSet -> {
                    if(resultSet.getRows(true).isEmpty())
                        fail("Database has no data");
                    assertCompany(companies.remove(0), resultSet.getRows(true).get(0));
                })
                .join();
    }

    @Test
    public void testEmbeddedIdFindWhere() {
        List<Car> cars = carMapper.find(new EqualAndCondition<>("brand", "Mitsubishi")).join();

        SQLConnection con = unit.getConnection().join();

        SqlUtils.<ResultSet>callbackToPromise(ar ->
                con.queryWithParams("select owner, plate, brand, model, CAST(version as bigint) version from Car where brand = ?",
                        new JsonArray().add("Mitsubishi"), ar))
                .thenAccept(resultSet -> {
                    if(resultSet.getRows(true).isEmpty())
                        fail("Database has no data");
                    assertCar(cars.remove(0), resultSet.getRows(true).get(0));
                })
                .thenAccept(v -> SqlUtils.callbackToPromise(con::rollback))
                .join();
    }

    @Test
    public void testNNExternalFindWhere(){
        Book book = bookMapper.find(new EqualAndCondition<>("name", "1001 noites")).join().get(0);

        assertSingleRow(book, bookSelectQuery, new JsonArray().add(book.getName()), (book1, rs) -> AssertUtils.assertBook(book1, rs, unit), unit.getConnection().join());
    }

    @Test
    public void testParentExternalFindWhere(){
        Employee employee = employeeMapper.find(new EqualAndCondition<>("name", "Bob")).join().get(0);
        assertSingleRow(employee, employeeSelectQuery, new JsonArray().add("Bob"), AssertUtils::assertEmployee, unit.getConnection().join());
    }

    @Test
    public void testPaginationFindWhere() {
        List<Company> companies = companyMapper.find(0, 10, new EqualAndCondition<>("id", 1)).join();
        assertMultipleRows(unit.getConnection().join(), companies, companySelectTop10Query, AssertUtils::assertCompany, 10);
    }

    @Test
    public void testSecondPageFindWhere() {
        List<Company> companies = companyMapper.find(1, 10, new EqualAndCondition<>("id", 1)).join();
        assertEquals(1, companies.size());
        assertSingleRow(companies.get(0), companySelectQuery, new JsonArray().add(1).add(11), AssertUtils::assertCompany, unit.getConnection().join());
    }

    //-----------------------------------FindById-----------------------------------//
    @Test
    public void testSimpleFindById(){
        int nif = 321;
        Person person = personMapper
                .findById(nif)
                .join()
                .orElseThrow(() -> new AssertionError(detailMessage));
        assertSingleRow(person, personSelectQuery, new JsonArray().add(nif), AssertUtils::assertPerson, unit.getConnection().join());
    }

    @Test
    public void testEmbeddedIdFindById() {
        SQLConnection con = unit.getConnection().join();
        int owner = 2; String plate = "23we45";
        Car car = carMapper
                .findById(new CarKey(owner, plate))
                .join()
                .orElseThrow(() -> new AssertionError(detailMessage));
        assertSingleRow(car, carSelectQuery, new JsonArray().add(owner).add(plate), AssertUtils::assertCar, con);
    }

    @Test
    public void testHierarchyFindById() {
        SQLConnection con = unit.getConnection().join();
        TopStudent topStudent = topStudentMapper
                .findById(454)
                .join()
                .orElseThrow(() -> new AssertionError(detailMessage));
        assertSingleRow(topStudent, topStudentSelectQuery, new JsonArray().add(454), AssertUtils::assertTopStudent, con);
    }

    @Test
    public void testEmbeddedIdExternalFindById(){
        SQLConnection con = unit.getConnection().join();
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
        List<Person> people = personMapper.find().join();
        assertMultipleRows(unit.getConnection().join(), people, "select nif, name, birthday, CAST(version as bigint) version from Person", AssertUtils::assertPerson, 2);
    }

    @Test
    public void testEmbeddedIdFindAll() {
        SQLConnection con = unit.getConnection().join();
        List<Car> cars = carMapper.find().join();
        assertMultipleRows(con, cars, "select owner, plate, brand, model, CAST(version as bigint) version from Car", AssertUtils::assertCar, 1);
    }

    @Test
    public void testHierarchyFindAll(){
        List<TopStudent> topStudents = topStudentMapper.find().join();
        assertMultipleRows(unit.getConnection().join(), topStudents, topStudentSelectQuery.substring(0, topStudentSelectQuery.length() - 15), AssertUtils::assertTopStudent, 1);
    }

    @Test
    public void testPaginationFindAll() {
        SQLConnection con = unit.getConnection().join();
        List<Company> companies = companyMapper.find(0, 10).join();
        assertMultipleRows(con, companies, companySelectTop10Query, AssertUtils::assertCompany, 10);
    }

    //-----------------------------------Create-----------------------------------//
    @Test
    public void testSimpleCreate() {
        SQLConnection con = unit.getConnection().join();
        Person person = new Person(123, "abc", new Date(1969, 6, 9).toInstant(), 0);
        personMapper.create(person).join();
        assertSingleRow(person, personSelectQuery, new JsonArray().add(person.getNif()), AssertUtils::assertPerson, con);
    }

    @Test
    public void testEmbeddedIdCreate() {
        SQLConnection con = unit.getConnection().join();
        Car car = new Car(1, "58en60", "Mercedes", "ES1", 0);
        carMapper.create(car).join();
        assertSingleRow(car, carSelectQuery, new JsonArray().add(car.getIdentityKey().getOwner()).add(car.getIdentityKey().getPlate()), AssertUtils::assertCar, con);
    }

    @Test
    public void testHierarchyCreate() {
        SQLConnection con = unit.getConnection().join();
        TopStudent topStudent = new TopStudent(456, "Manel", new Date(2020, 12, 1).toInstant(), 0, 1, 20, 2016, 0, 0);
        topStudentMapper.create(topStudent).join();
        assertSingleRow(topStudent, topStudentSelectQuery, new JsonArray().add(topStudent.getNif()), AssertUtils::assertTopStudent, con);
    }

    @Test
    public void testSingleExternalCreate() {
        SQLConnection con = unit.getConnection().join();
        CompletableFuture<Company> companyCompletableFuture = companyMapper
                .findById(new Company.PrimaryKey(1, 1))
                .thenApply(company -> company.orElseThrow(() -> new DataMapperException(("Company not found"))));

        Employee employee = new Employee(0, "Hugo", 0, new Foreign<>( new Company.PrimaryKey(1, 1), uW -> companyCompletableFuture));
        employeeMapper.create(employee).join();
        assertSingleRow(employee, employeeSelectQuery, new JsonArray().add("Hugo"), AssertUtils::assertEmployee, con);
    }

    @Test
    public void testSingleExternalNullCreate() {
        SQLConnection con = unit.getConnection().join();
        Employee employee = new Employee(0, "Hugo", 0, null);
        employeeMapper.create(employee)
                .join();
        assertSingleRow(employee, employeeSelectQuery, new JsonArray().add("Hugo"), AssertUtils::assertEmployee, con);
    }

    @Test
    public void testNoVersionCreate() {
        SQLConnection con = unit.getConnection().join();
        Dog dog = new Dog(new Dog.DogPK("Bobby", "Pitbull"), 5);
        dogMapper.create(dog)
                .join();
        assertSingleRow(dog, dogSelectQuery, new JsonArray().add("Bobby").add("Pitbull"), AssertUtils::assertDog, con);
    }

    //-----------------------------------Update-----------------------------------//
    @Test
    public void testSimpleUpdate() {
        SQLConnection con = unit.getConnection().join();
        ResultSet rs = executeQuery("select CAST(version as bigint) version from Person where nif = ?", new JsonArray().add(321), con);
        Person person = new Person(321, "Maria", new Date(2010, 2, 3).toInstant(), rs.getResults().get(0).getLong(0));

        personMapper.update(person).join();

        assertSingleRow(person, personSelectQuery, new JsonArray().add(person.getNif()), AssertUtils::assertPerson, con);
    }

    @Test
    public void testEmbeddedIdUpdate() {
        SQLConnection con = unit.getConnection().join();
        ResultSet rs = executeQuery("select CAST(version as bigint) version from Car where owner = ? and plate = ?", new JsonArray().add(2).add( "23we45"), con);
        Car car = new Car(2, "23we45", "Mitsubishi", "lancer evolution", rs.getResults().get(0).getLong(0));

        carMapper.update(car).join();

        assertSingleRow(car, carSelectQuery, new JsonArray().add(car.getIdentityKey().getOwner()).add(car.getIdentityKey().getPlate()), AssertUtils::assertCar, con);
    }

    @Test
    public void testHierarchyUpdate() {
        SQLConnection con = unit.getConnection().join();
        ResultSet rs = executeQuery("select CAST(P.version as bigint), CAST(S2.version as bigint), CAST(TS.version as bigint) version from Person P " +
                "inner join Student S2 on P.nif = S2.nif " +
                "inner join TopStudent TS on S2.nif = TS.nif where P.nif = ?", new JsonArray().add(454), con);
        JsonArray first = rs.getResults().get(0);
        TopStudent topStudent = new TopStudent(454, "Carlos", new Date(2010, 6, 3).toInstant(), first.getLong(1),
                4, 6, 7, first.getLong(2), first.getLong(0));

        topStudentMapper.update(topStudent).join();

        assertSingleRow(topStudent, topStudentSelectQuery, new JsonArray().add(topStudent.getNif()), AssertUtils::assertTopStudent, con);
    }

    @Test
    public void testSingleExternalUpdate() {
        SQLConnection con = unit.getConnection().join();
        ResultSet rs = executeQuery(employeeSelectQuery, new JsonArray().add("Bob"), con);
        JsonObject first = rs.getRows(true).get(0);
        Employee employee = new Employee(first.getInteger("id"),"Boba", first.getLong("version"),
                new Foreign<>(new Company.PrimaryKey(1, 2), uW -> companyMapper
                        .findById(new Company.PrimaryKey(1, 2))
                        .thenApply(company -> company.orElseThrow(() -> new DataMapperException(("Company not found"))))));

        employeeMapper.update(employee).join();

        assertSingleRow(employee, employeeSelectQuery, new JsonArray().add("Boba"), AssertUtils::assertEmployee, con);
    }

    @Test
    public void testSingleExternalNullUpdate() {
        SQLConnection con = unit.getConnection().join();
        ResultSet rs = executeQuery(employeeSelectQuery, new JsonArray().add("Bob"), con);
        JsonObject first = rs.getRows(true).get(0);
        Employee employee = new Employee(
                first.getInteger("id"),"Boba",
                first.getLong("version"), null);

        employeeMapper.update(employee).join();

        assertSingleRow(employee, employeeSelectQuery, new JsonArray().add("Boba"), AssertUtils::assertEmployee, con);
    }

    @Test
    public void testNoVersionUpdate() {
        SQLConnection con = unit.getConnection().join();
        ResultSet rs = executeQuery(dogSelectQuery, new JsonArray().add("Doggy").add("Bulldog"), con);
        JsonObject first = rs.getRows(true).get(0);
        Dog dog = new Dog(
                new Dog.DogPK(
                        first.getString("name"),
                        first.getString("race")), 6);
        dogMapper.update(dog).join();
        assertSingleRow(dog, dogSelectQuery, new JsonArray().add("Doggy").add("Bulldog"), AssertUtils::assertDog, con);
    }

    //-----------------------------------DeleteById-----------------------------------//
    @Test
    public void testSimpleDeleteById() {
        SQLConnection con = unit.getConnection().join();
        personMapper.deleteById(321).join();
        assertNotFound(personSelectQuery, new JsonArray().add(321), con);
    }

    @Test
    public void testEmbeddedDeleteById() {
        SQLConnection con = unit.getConnection().join();
        carMapper.deleteById(new CarKey(2, "23we45")).join();
        assertNotFound(carSelectQuery, new JsonArray().add(2).add("23we45"), con);
    }

    @Test
    public void testHierarchyDeleteById() {
        SQLConnection con = unit.getConnection().join();
        topStudentMapper.deleteById(454).join();
        assertNotFound(topStudentSelectQuery, new JsonArray().add(454), con);
    }

    @Test
    public void testSingleExternalDeleteById() {
        SQLConnection con = unit.getConnection().join();
        ResultSet rs = executeQuery(employeeSelectQuery, new JsonArray().add("Bob"), con);
        employeeMapper.deleteById(rs.getRows(true).get(0).getInteger("id")).join();
        assertNotFound(employeeSelectQuery, new JsonArray().add("Bob"), con);
    }

    //-----------------------------------Delete-----------------------------------//
    @Test
    public void testSimpleDelete() {
        SQLConnection con = unit.getConnection().join();
        Person person = new Person(321, null, null, 0);
        personMapper.delete(person).join();
        assertNotFound(personSelectQuery, new JsonArray().add(321), con);
    }

    @Test
    public void testEmbeddedDelete() {
        SQLConnection con = unit.getConnection().join();
        Car car = new Car(2, "23we45", null, null, 0);
        carMapper.delete(car).join();
        assertNotFound(carSelectQuery, new JsonArray().add(2).add( "23we45"), con);
    }

    @Test
    public void testHierarchyDelete() {
        SQLConnection con = unit.getConnection().join();
        TopStudent topStudent = new TopStudent(454, null, null, 0, 0, 0, 0, 0, 0);
        topStudentMapper.delete(topStudent).join();
        assertNotFound(topStudentSelectQuery, new JsonArray().add(454), con);
    }

    @Test
    public void testSingleExternalDelete() {
        SQLConnection con = unit.getConnection().join();
        ResultSet rs = executeQuery(employeeSelectQuery, new JsonArray().add("Bob"), con);
        JsonObject first = rs.getRows(true).get(0);
        Employee employee = new Employee(first.getInteger("id"), first.getString("name"), first.getLong("version"),
                new Foreign<>(new Company.PrimaryKey(1, 2), uW -> companyMapper
                        .findById(new Company.PrimaryKey(1, 2))
                        .thenApply(company -> company.orElseThrow(() -> new DataMapperException(("Company not found"))))));
        employeeMapper.delete(employee).join();
        assertNotFound(employeeSelectQuery, new JsonArray().add("Bob"), con);
    }
}
