package com.github.jayield.rapper;

import com.github.jayield.rapper.domainModel.*;
import com.github.jayield.rapper.utils.*;
import javafx.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.sql.Date;
import java.util.*;

import static com.github.jayield.rapper.AssertUtils.*;
import static com.github.jayield.rapper.TestUtils.*;
import static com.github.jayield.rapper.utils.DBsPath.TESTDB;
import static org.junit.Assert.*;

public class DataMapperTests {
    private static String detailMessage = "DomainObject wasn't found";

    private final Logger logger = LoggerFactory.getLogger(DataMapperTests.class);

    private final DataMapper<Person, Integer> personMapper = (DataMapper<Person, Integer>) MapperRegistry.getRepository(Person.class).getMapper();
    private final DataMapper<Car, Car.PrimaryPk> carMapper = (DataMapper<Car, Car.PrimaryPk>) MapperRegistry.getRepository(Car.class).getMapper();
    private final DataMapper<TopStudent, Integer> topStudentMapper = (DataMapper<TopStudent, Integer>) MapperRegistry.getRepository(TopStudent.class).getMapper();
    private final DataMapper<Company, Company.PrimaryKey> companyMapper = (DataMapper<Company, Company.PrimaryKey>) MapperRegistry.getRepository(Company.class).getMapper();
    private final DataMapper<Book, Long> bookMapper = (DataMapper<Book, Long>) MapperRegistry.getRepository(Book.class).getMapper();
    private final DataMapper<Employee, Integer> employeeMapper = (DataMapper<Employee, Integer>) MapperRegistry.getRepository(Employee.class).getMapper();
    private Connection con;

    @Before
    public void start() throws SQLException {
        ConnectionManager manager = ConnectionManager.getConnectionManager(TESTDB);
        SqlSupplier<Connection> connectionSupplier = manager::getConnection;
        UnitOfWork.newCurrent(connectionSupplier.wrap());
        con = UnitOfWork.getCurrent().getConnection();
        con.prepareCall("{call deleteDB}").execute();
        con.prepareCall("{call populateDB}").execute();
        con.commit();
    }

    @After
    public void finish() throws SQLException {
        con.rollback();
        con.close();
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
        Book book = bookMapper.findWhere(new Pair<>("name", "1001 noites")).join().get(0);
        assertSingleRow(book, bookSelectQuery, getBookPSConsumer(book.getName()), (book1, rs) -> AssertUtils.assertBook(book1, rs, con), con);
    }

    @Test
    public void testParentExternalFindWhere(){
        Employee employee = employeeMapper.findWhere(new Pair<>("name", "Bob")).join().get(0);
        assertSingleRow(employee, employeeSelectQuery, getEmployeePSConsumer("Bob"), AssertUtils::assertEmployee, con);
    }

    //-----------------------------------FindById-----------------------------------//
    @Test
    public void testSimpleFindById(){
        int nif = 321;
        Person person = personMapper
                .findById(nif)
                .join()
                .orElseThrow(() -> new AssertionError(detailMessage));
        assertSingleRow(person, personSelectQuery, getPersonPSConsumer(nif), AssertUtils::assertPerson, con);
    }

    @Test
    public void testEmbeddedIdFindById(){
        int owner = 2; String plate = "23we45";
        Car car = carMapper
                .findById(new Car.PrimaryPk(owner, plate))
                .join()
                .orElseThrow(() -> new AssertionError(detailMessage));
        assertSingleRow(car, carSelectQuery, getCarPSConsumer(owner, plate), AssertUtils::assertCar, con);
    }

    @Test
    public void testHierarchyFindById(){
        TopStudent topStudent = topStudentMapper
                .findById(454)
                .join()
                .orElseThrow(() -> new AssertionError(detailMessage));
        assertSingleRow(topStudent, topStudentSelectQuery, getPersonPSConsumer(454), AssertUtils::assertTopStudent, con);
    }

    @Test
    public void testEmbeddedIdExternalFindById(){
        int companyId = 1, companyCid = 1;
        Company company = companyMapper
                .findById(new Company.PrimaryKey(companyId, companyCid))
                .join()
                .orElseThrow(() -> new AssertionError(detailMessage));
        assertSingleRow(company, "select id, cid, motto, CAST(version as bigint) version from Company where id = ? and cid = ?",
                getCompanyPSConsumer(companyId, companyCid), (company1, resultSet) -> assertCompany(company1, resultSet, con), con);
    }

    //-----------------------------------FindAll-----------------------------------//
    @Test
    public void testSimpleFindAll(){
        List<Person> people = personMapper.findAll().join();
        assertMultipleRows(con, people, "select nif, name, birthday, CAST(version as bigint) version from Person", AssertUtils::assertPerson, 2);
    }

    @Test
    public void testEmbeddedIdFindAll(){
        List<Car> cars = carMapper.findAll().join();
        assertMultipleRows(con, cars, "select owner, plate, brand, model, CAST(version as bigint) version from Car", AssertUtils::assertCar, 1);
    }

    @Test
    public void testHierarchyFindAll(){
        List<TopStudent> topStudents = topStudentMapper.findAll().join();
        assertMultipleRows(con, topStudents, topStudentSelectQuery.substring(0, topStudentSelectQuery.length() - 15), AssertUtils::assertTopStudent, 1);
    }

    //-----------------------------------Create-----------------------------------//
    @Test
    public void testSimpleCreate(){
        Person person = new Person(123, "abc", new Date(1969, 6, 9), 0);
        assertTrue(personMapper.create(person).join());
        assertSingleRow(person, personSelectQuery, getPersonPSConsumer(person.getNif()), AssertUtils::assertPerson, con);
    }

    @Test
    public void testEmbeddedIdCreate(){
        Car car = new Car(1, "58en60", "Mercedes", "ES1", 0);
        assertTrue(carMapper.create(car).join());
        assertSingleRow(car, carSelectQuery, getCarPSConsumer(car.getIdentityKey().getOwner(), car.getIdentityKey().getPlate()), AssertUtils::assertCar, con);
    }

    @Test
    public void testHierarchyCreate(){
        TopStudent topStudent = new TopStudent(456, "Manel", new Date(2020, 12, 1), 0, 1, 20, 2016, 0, 0);
        assertTrue(topStudentMapper.create(topStudent).join());
        assertSingleRow(topStudent, topStudentSelectQuery, getPersonPSConsumer(topStudent.getNif()), AssertUtils::assertTopStudent, con);
    }

    //-----------------------------------Update-----------------------------------//
    @Test
    public void testSimpleUpdate() throws SQLException {
        ResultSet rs = executeQuery("select CAST(version as bigint) version from Person where nif = ?", getPersonPSConsumer(321));
        Person person = new Person(321, "Maria", new Date(2010, 2, 3), rs.getLong(1));

        assertTrue(personMapper.update(person).join());

        assertSingleRow(person, personSelectQuery, getPersonPSConsumer(person.getNif()), AssertUtils::assertPerson, con);
    }

    @Test
    public void testEmbeddedIdUpdate() throws SQLException {
        ResultSet rs = executeQuery("select CAST(version as bigint) version from Car where owner = ? and plate = ?", getCarPSConsumer(2, "23we45"));
        Car car = new Car(2, "23we45", "Mitsubishi", "lancer evolution", rs.getLong(1));

        assertTrue(carMapper.update(car).join());

        assertSingleRow(car, carSelectQuery, getCarPSConsumer(car.getIdentityKey().getOwner(), car.getIdentityKey().getPlate()), AssertUtils::assertCar, con);
    }

    @Test
    public void testHierarchyUpdate() throws SQLException {
        ResultSet rs = executeQuery("select CAST(P.version as bigint), CAST(S2.version as bigint), CAST(TS.version as bigint) version from Person P " +
                "inner join Student S2 on P.nif = S2.nif " +
                "inner join TopStudent TS on S2.nif = TS.nif where P.nif = ?", getPersonPSConsumer(454));
        TopStudent topStudent = new TopStudent(454, "Carlos", new Date(2010, 6, 3), rs.getLong(2),
                4, 6, 7, rs.getLong(3), rs.getLong(1));

        assertTrue(topStudentMapper.update(topStudent).join());

        assertSingleRow(topStudent, topStudentSelectQuery, getPersonPSConsumer(topStudent.getNif()), AssertUtils::assertTopStudent, con);
    }

    //-----------------------------------DeleteById-----------------------------------//
    @Test
    public void testSimpleDeleteById(){
        assertTrue(personMapper.deleteById(321).join());
        assertNotFound(personSelectQuery, getPersonPSConsumer(321), con);
    }

    @Test
    public void testEmbeddedDeleteById(){
        assertTrue(carMapper.deleteById(new Car.PrimaryPk(2, "23we45")).join());
        assertNotFound(carSelectQuery, getCarPSConsumer(2, "23we45"), con);
    }

    @Test
    public void testHierarchyDeleteById(){
        assertTrue(topStudentMapper.deleteById(454).join());
        assertNotFound(topStudentSelectQuery, getPersonPSConsumer(454), con);
    }

    //-----------------------------------Delete-----------------------------------//
    @Test
    public void testSimpleDelete(){
        Person person = new Person(321, null, null, 0);
        assertTrue(personMapper.delete(person).join());
        assertNotFound(personSelectQuery, getPersonPSConsumer(321), con);
    }

    @Test
    public void testEmbeddedDelete(){
        Car car = new Car(2, "23we45", null, null, 0);
        assertTrue(carMapper.delete(car).join());
        assertNotFound(carSelectQuery, getCarPSConsumer(2, "23we45"), con);
    }

    @Test
    public void testHierarchyDelete(){
        TopStudent topStudent = new TopStudent(454, null, null, 0, 0, 0, 0, 0, 0);
        assertTrue(topStudentMapper.delete(topStudent).join());
        assertNotFound(topStudentSelectQuery, getPersonPSConsumer(454), con);
    }
}
