package com.github.jayield.rapper;

import com.github.jayield.rapper.domainModel.*;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.utils.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static com.github.jayield.rapper.AssertUtils.*;
import static com.github.jayield.rapper.TestUtils.*;
import static org.junit.Assert.*;

public class DataMapperTests {
    private static String detailMessage = "DomainObject wasn't found";

    private final Logger logger = LoggerFactory.getLogger(DataMapperTests.class);

    private final DataMapper<Person, Integer> personMapper = (DataMapper<Person, Integer>) MapperRegistry.getRepository(Person.class).getMapper();
    private final DataMapper<Car, CarKey> carMapper = (DataMapper<Car, CarKey>) MapperRegistry.getRepository(Car.class).getMapper();
    private final DataMapper<TopStudent, Integer> topStudentMapper = (DataMapper<TopStudent, Integer>) MapperRegistry.getRepository(TopStudent.class).getMapper();
    private final DataMapper<Company, Company.PrimaryKey> companyMapper = (DataMapper<Company, Company.PrimaryKey>) MapperRegistry.getRepository(Company.class).getMapper();
    private final DataMapper<Book, Long> bookMapper = (DataMapper<Book, Long>) MapperRegistry.getRepository(Book.class).getMapper();
    private final DataMapper<Employee, Integer> employeeMapper = (DataMapper<Employee, Integer>) MapperRegistry.getRepository(Employee.class).getMapper();
    private final DataMapper<Dog, Dog.DogPK> dogMapper = (DataMapper<Dog, Dog.DogPK>) MapperRegistry.getRepository(Dog.class).getMapper();
    //private Connection con;

    @Before
    public void start() throws SQLException {
        ConnectionManager manager = ConnectionManager.getConnectionManager(
                "jdbc:hsqldb:file:"+URLDecoder.decode(this.getClass().getClassLoader().getResource("testdb").getPath())+"/testdb",
                "SA", "");
        SqlSupplier<Connection> connectionSupplier = manager::getConnection;
        UnitOfWork.newCurrent(connectionSupplier.wrap());
        Connection con = UnitOfWork.getCurrent().getConnection();

        con.prepareCall("{call deleteDB()}").execute();
        con.prepareCall("{call populateDB()}").execute();
        con.commit();
    }

    @After
    public void finish() throws SQLException {
        UnitOfWork.getCurrent().getConnection().rollback();
        UnitOfWork.getCurrent().getConnection().close();
    }

    //-----------------------------------FindWhere-----------------------------------//
    @Test
    public void testSimpleFindWhere() throws SQLException {
        List<Person> people = personMapper.findWhere(new Pair<>("name", "Jose")).join();

        PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select nif, name, birthday, CAST(version as bigint) version from Person where name = ?");
        ps.setString(1, "Jose");
        ResultSet rs = ps.executeQuery();

        if (rs.next())
            assertPerson(people.remove(0), rs);
        else fail("Database has no data");
    }

    @Test
    public void testEmbeddedIdFindWhere() throws SQLException {
        List<Car> cars = carMapper.findWhere(new Pair<>("brand", "Mitsubishi")).join();

        PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select owner, plate, brand, model, CAST(version as bigint) version from Car where brand = ?");
        ps.setString(1, "Mitsubishi");
        ResultSet rs = ps.executeQuery();

        if (rs.next())
            assertCar(cars.remove(0), rs);
        else fail("Database has no data");
    }

    @Test
    public void testNNExternalFindWhere(){
        Connection con = UnitOfWork.getCurrent().getConnection();
        Book book = bookMapper.findWhere(new Pair<>("name", "1001 noites")).join().get(0);
        assertSingleRow(book, bookSelectQuery, getBookPSConsumer(book.getName()), (book1, rs) -> AssertUtils.assertBook(book1, rs, con ), con);
    }

    @Test
    public void testParentExternalFindWhere(){
        Employee employee = employeeMapper.findWhere(new Pair<>("name", "Bob")).join().get(0);
        assertSingleRow(employee, employeeSelectQuery, getEmployeePSConsumer("Bob"), AssertUtils::assertEmployee, UnitOfWork.getCurrent().getConnection());
    }

    @Test
    public void testPaginationFindWhere() {
        List<Company> companies = companyMapper.findWhere(0, 10, new Pair<>("id", 1)).join();
        assertMultipleRows(UnitOfWork.getCurrent().getConnection(), companies, companySelectTop10Query, AssertUtils::assertCompany, 10);
    }

    @Test
    public void testSecondPageFindWhere() {
        List<Company> companies = companyMapper.findWhere(1, 10, new Pair<>("id", 1)).join();
        assertEquals(1, companies.size());
        assertSingleRow(companies.get(0), companySelectQuery, getCompanyPSConsumer(1, 11), AssertUtils::assertCompany, UnitOfWork.getCurrent().getConnection());
    }

    //-----------------------------------FindById-----------------------------------//
    @Test
    public void testSimpleFindById(){
        int nif = 321;
        Person person = personMapper
                .findById(nif)
                .join()
                .orElseThrow(() -> new AssertionError(detailMessage));
        assertSingleRow(person, personSelectQuery, getPersonPSConsumer(nif), AssertUtils::assertPerson, UnitOfWork.getCurrent().getConnection());
    }

    @Test
    public void testEmbeddedIdFindById(){
        int owner = 2; String plate = "23we45";
        Car car = carMapper
                .findById(new CarKey(owner, plate))
                .join()
                .orElseThrow(() -> new AssertionError(detailMessage));
        assertSingleRow(car, carSelectQuery, getCarPSConsumer(owner, plate), AssertUtils::assertCar, UnitOfWork.getCurrent().getConnection());
    }

    @Test
    public void testHierarchyFindById(){
        TopStudent topStudent = topStudentMapper
                .findById(454)
                .join()
                .orElseThrow(() -> new AssertionError(detailMessage));
        assertSingleRow(topStudent, topStudentSelectQuery, getPersonPSConsumer(454), AssertUtils::assertTopStudent, UnitOfWork.getCurrent().getConnection());
    }

    @Test
    public void testEmbeddedIdExternalFindById(){
        Connection con = UnitOfWork.getCurrent().getConnection();
        int companyId = 1, companyCid = 1;
        Company company = companyMapper
                .findById(new Company.PrimaryKey(companyId, companyCid))
                .join()
                .orElseThrow(() -> new AssertionError(detailMessage));
        assertSingleRow(company, "select id, cid, motto, CAST(version as bigint) Cversion from Company where id = ? and cid = ?",
                getCompanyPSConsumer(companyId, companyCid), AssertUtils::assertCompany, con);
    }

    //-----------------------------------FindAll-----------------------------------//
    @Test
    public void testSimpleFindAll(){
        List<Person> people = personMapper.findAll().join();
        assertMultipleRows(UnitOfWork.getCurrent().getConnection(), people, "select nif, name, birthday, CAST(version as bigint) version from Person", AssertUtils::assertPerson, 2);
    }

    @Test
    public void testEmbeddedIdFindAll(){
        List<Car> cars = carMapper.findAll().join();
        assertMultipleRows(UnitOfWork.getCurrent().getConnection(), cars, "select owner, plate, brand, model, CAST(version as bigint) version from Car", AssertUtils::assertCar, 1);
    }

    @Test
    public void testHierarchyFindAll(){
        List<TopStudent> topStudents = topStudentMapper.findAll().join();
        assertMultipleRows(UnitOfWork.getCurrent().getConnection(), topStudents, topStudentSelectQuery.substring(0, topStudentSelectQuery.length() - 15), AssertUtils::assertTopStudent, 1);
    }

    @Test
    public void testPaginationFindAll() {
        List<Company> companies = companyMapper.findAll(0, 10).join();
        assertMultipleRows(UnitOfWork.getCurrent().getConnection(), companies, companySelectTop10Query, AssertUtils::assertCompany, 10);
    }

    //-----------------------------------Create-----------------------------------//
    @Test
    public void testSimpleCreate(){
        Person person = new Person(123, "abc", new Date(1969, 6, 9), 0);
        personMapper.create(person)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertSingleRow(person, personSelectQuery, getPersonPSConsumer(person.getNif()), AssertUtils::assertPerson, UnitOfWork.getCurrent().getConnection());
    }

    @Test
    public void testEmbeddedIdCreate(){
        Car car = new Car(1, "58en60", "Mercedes", "ES1", 0);
        carMapper.create(car)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertSingleRow(car, carSelectQuery, getCarPSConsumer(car.getIdentityKey().getOwner(), car.getIdentityKey().getPlate()), AssertUtils::assertCar, UnitOfWork.getCurrent().getConnection());
    }

    @Test
    public void testHierarchyCreate(){
        TopStudent topStudent = new TopStudent(456, "Manel", new Date(2020, 12, 1), 0, 1, 20, 2016, 0, 0);
        topStudentMapper.create(topStudent)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertSingleRow(topStudent, topStudentSelectQuery, getPersonPSConsumer(topStudent.getNif()), AssertUtils::assertTopStudent, UnitOfWork.getCurrent().getConnection());
    }

    @Test
    public void testSingleExternalCreate(){
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
        assertSingleRow(employee, employeeSelectQuery, getEmployeePSConsumer("Hugo"), AssertUtils::assertEmployee, UnitOfWork.getCurrent().getConnection());
    }

    @Test
    public void testSingleExternalNullCreate(){
            Employee employee = new Employee(0, "Hugo", 0, null);
            employeeMapper.create(employee)
                    .exceptionally(throwable -> {
                        fail(throwable.getMessage());
                        return null;
                    })
                    .join();
            assertSingleRow(employee, employeeSelectQuery, getEmployeePSConsumer("Hugo"), AssertUtils::assertEmployee, UnitOfWork.getCurrent().getConnection());
    }

    @Test
    public void testNoVersionCreate(){
        Dog dog = new Dog(new Dog.DogPK("Bobby", "Pitbull"), 5);
        dogMapper.create(dog)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertSingleRow(dog, dogSelectQuery, getDogPSConsumer("Bobby", "Pitbull"), AssertUtils::assertDog, UnitOfWork.getCurrent().getConnection());
    }

    //-----------------------------------Update-----------------------------------//
    @Test
    public void testSimpleUpdate() throws SQLException {
        ResultSet rs = executeQuery("select CAST(version as bigint) version from Person where nif = ?", getPersonPSConsumer(321), UnitOfWork.getCurrent().getConnection());
        Person person = new Person(321, "Maria", new Date(2010, 2, 3), rs.getLong(1));

        personMapper.update(person)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();

        assertSingleRow(person, personSelectQuery, getPersonPSConsumer(person.getNif()), AssertUtils::assertPerson, UnitOfWork.getCurrent().getConnection());
    }

    @Test
    public void testEmbeddedIdUpdate() throws SQLException {
        ResultSet rs = executeQuery("select CAST(version as bigint) version from Car where owner = ? and plate = ?", getCarPSConsumer(2, "23we45"), UnitOfWork.getCurrent().getConnection());
        Car car = new Car(2, "23we45", "Mitsubishi", "lancer evolution", rs.getLong(1));

        carMapper.update(car)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();

        assertSingleRow(car, carSelectQuery, getCarPSConsumer(car.getIdentityKey().getOwner(), car.getIdentityKey().getPlate()), AssertUtils::assertCar, UnitOfWork.getCurrent().getConnection());
    }

    @Test
    public void testHierarchyUpdate() throws SQLException {
        ResultSet rs = executeQuery("select CAST(P.version as bigint), CAST(S2.version as bigint), CAST(TS.version as bigint) version from Person P " +
                "inner join Student S2 on P.nif = S2.nif " +
                "inner join TopStudent TS on S2.nif = TS.nif where P.nif = ?", getPersonPSConsumer(454), UnitOfWork.getCurrent().getConnection());
        TopStudent topStudent = new TopStudent(454, "Carlos", new Date(2010, 6, 3), rs.getLong(2),
                4, 6, 7, rs.getLong(3), rs.getLong(1));

        topStudentMapper.update(topStudent)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();

        assertSingleRow(topStudent, topStudentSelectQuery, getPersonPSConsumer(topStudent.getNif()), AssertUtils::assertTopStudent, UnitOfWork.getCurrent().getConnection());
    }

    @Test
    public void testSingleExternalUpdate() throws SQLException {
        ResultSet rs = executeQuery(employeeSelectQuery, getEmployeePSConsumer("Bob"), UnitOfWork.getCurrent().getConnection());
        Employee employee = new Employee(rs.getInt("id"),"Boba", rs.getLong("version"),
                companyMapper
                        .findById(new Company.PrimaryKey(1, 2))
                        .thenApply(company -> company.orElseThrow(() -> new DataMapperException(("Company not found")))));

        employeeMapper.update(employee)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();

        assertSingleRow(employee, employeeSelectQuery, getEmployeePSConsumer("Boba"), AssertUtils::assertEmployee, UnitOfWork.getCurrent().getConnection());
    }

    @Test
    public void testSingleExternalNullUpdate() throws SQLException {
        ResultSet rs = executeQuery(employeeSelectQuery, getEmployeePSConsumer("Bob"), UnitOfWork.getCurrent().getConnection());
        Employee employee = new Employee(rs.getInt("id"),"Boba", rs.getLong("version"), null);

        employeeMapper.update(employee)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();

        assertSingleRow(employee, employeeSelectQuery, getEmployeePSConsumer("Boba"), AssertUtils::assertEmployee, UnitOfWork.getCurrent().getConnection());
    }

    @Test
    public void testNoVersionUpdate() throws SQLException {
        ResultSet rs = executeQuery(dogSelectQuery, getDogPSConsumer("Doggy", "Bulldog"), UnitOfWork.getCurrent().getConnection());

        Dog dog = new Dog(
                new Dog.DogPK(
                        rs.getString("name"),
                        rs.getString("race")), 6);
        dogMapper.update(dog)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertSingleRow(dog, dogSelectQuery, getDogPSConsumer("Doggy", "Bulldog"), AssertUtils::assertDog, UnitOfWork.getCurrent().getConnection());
    }

    //-----------------------------------DeleteById-----------------------------------//
    @Test
    public void testSimpleDeleteById(){
        personMapper.deleteById(321)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertNotFound(personSelectQuery, getPersonPSConsumer(321), UnitOfWork.getCurrent().getConnection());
    }

    @Test
    public void testEmbeddedDeleteById(){
        carMapper.deleteById(new CarKey(2, "23we45"))
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertNotFound(carSelectQuery, getCarPSConsumer(2, "23we45"), UnitOfWork.getCurrent().getConnection());
    }

    @Test
    public void testHierarchyDeleteById(){
        topStudentMapper.deleteById(454)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertNotFound(topStudentSelectQuery, getPersonPSConsumer(454), UnitOfWork.getCurrent().getConnection());
    }

    @Test
    public void testSingleExternalDeleteById() throws SQLException {
        ResultSet rs = executeQuery(employeeSelectQuery, getEmployeePSConsumer("Bob"), UnitOfWork.getCurrent().getConnection());
        employeeMapper.deleteById(rs.getInt("id"))
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertNotFound(employeeSelectQuery, getEmployeePSConsumer("Bob"), UnitOfWork.getCurrent().getConnection());
    }

    //-----------------------------------Delete-----------------------------------//
    @Test
    public void testSimpleDelete(){
        Person person = new Person(321, null, null, 0);
        personMapper.delete(person)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertNotFound(personSelectQuery, getPersonPSConsumer(321), UnitOfWork.getCurrent().getConnection());
    }

    @Test
    public void testEmbeddedDelete(){
        Car car = new Car(2, "23we45", null, null, 0);
        carMapper.delete(car)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertNotFound(carSelectQuery, getCarPSConsumer(2, "23we45"), UnitOfWork.getCurrent().getConnection());
    }

    @Test
    public void testHierarchyDelete(){
        TopStudent topStudent = new TopStudent(454, null, null, 0, 0, 0, 0, 0, 0);
        topStudentMapper.delete(topStudent)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertNotFound(topStudentSelectQuery, getPersonPSConsumer(454), UnitOfWork.getCurrent().getConnection());
    }

    @Test
    public void testSingleExternalDelete() throws SQLException {
        ResultSet rs = executeQuery(employeeSelectQuery, getEmployeePSConsumer("Bob"), UnitOfWork.getCurrent().getConnection());
        Employee employee = new Employee(rs.getInt("id"), rs.getString("name"), rs.getLong("version"),
                companyMapper
                        .findById(new Company.PrimaryKey(1, 2))
                        .thenApply(company -> company.orElseThrow(() -> new DataMapperException(("Company not found")))));
        employeeMapper.delete(employee)
                .exceptionally(throwable -> {
                    fail(throwable.getMessage());
                    return null;
                })
                .join();
        assertNotFound(employeeSelectQuery, getEmployeePSConsumer("Bob"), UnitOfWork.getCurrent().getConnection());
    }
}
