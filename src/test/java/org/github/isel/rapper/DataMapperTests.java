package org.github.isel.rapper;

import javafx.util.Pair;
import org.github.isel.rapper.domainModel.Car;
import org.github.isel.rapper.domainModel.Company;
import org.github.isel.rapper.domainModel.Person;
import org.github.isel.rapper.domainModel.TopStudent;
import org.github.isel.rapper.utils.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.github.isel.rapper.AssertUtils.*;
import static org.github.isel.rapper.TestUtils.*;
import static org.github.isel.rapper.utils.DBsPath.TESTDB;
import static org.junit.Assert.*;

/**
 * PersonMapper -> Simple Class with @Id annotation
 * CarMapper -> Simple Class with @EmbeddedId annotation
 * TopStudentMapper -> Class that extends Student and Person
 */
public class DataMapperTests {
    private final Logger logger = LoggerFactory.getLogger(DataMapperTests.class);

    private final DataMapper<Person, Integer> personMapper = MapperRegistry.getRepository(Person.class).getMapper();
    private final DataMapper<Car, Car.PrimaryPk> carMapper = MapperRegistry.getRepository(Car.class).getMapper();
    private final DataMapper<TopStudent, Integer> topStudentMapper = MapperRegistry.getRepository(TopStudent.class).getMapper();
    private final DataMapper<Company, Company.PrimaryKey> companyMapper = MapperRegistry.getRepository(Company.class).getMapper();

    @Before
    public void start() throws SQLException {
        ConnectionManager manager = ConnectionManager.getConnectionManager(TESTDB);
        UnitOfWork.newCurrent(manager::getConnection);
        Connection con = UnitOfWork.getCurrent().getConnection();
        con.prepareCall("{call deleteDB}").execute();
        con.prepareCall("{call populateDB}").execute();
        con.commit();
    }

    @After
    public void finish() {
        UnitOfWork.getCurrent().rollback();
        UnitOfWork.getCurrent().closeConnection();
    }

    @Test
    public void findWhere() {
        SqlConsumer<List<Person>> personConsumer = people -> {
            PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select nif, name, birthday, CAST(version as bigint) version from Person where name = ?");
            ps.setString(1, "Jose");
            ResultSet rs = ps.executeQuery();

            if (rs.next())
                assertPerson(people.remove(0), rs);
            else fail("Database has no data");
        };

        SqlConsumer<List<Car>> carConsumer = cars -> {
            PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select owner, plate, brand, model, CAST(version as bigint) version from Car where brand = ?");
            ps.setString(1, "Mitsubishi");
            ResultSet rs = ps.executeQuery();

            if (rs.next())
                assertCar(cars.remove(0), rs);
            else fail("Database has no data");
        };

        personMapper
                .findWhere(new Pair<>("name", "Jose"))
                .thenAccept(personConsumer.wrap());

        carMapper
                .findWhere(new Pair<>("brand", "Mitsubishi"))
                .thenAccept(carConsumer.wrap());
    }

    @Test
    public void findById() {
        int nif = 321;
        int owner = 2; String plate = "23we45";
        int companyId = 1, companyCid = 1;
        UnitOfWork current = UnitOfWork.getCurrent();

        List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
        completableFutures.add(personMapper
                .findById(nif)
                .thenApply(person -> person.orElse(new Person()))
                .thenAccept(person -> assertSingleRow(current, person, personSelectQuery,
                        getPersonPSConsumer(nif), AssertUtils::assertPerson)));

        completableFutures.add(carMapper
                .findById(new Car.PrimaryPk(owner, plate))
                .thenApply(car -> car.orElse(new Car()))
                .thenAccept(car -> assertSingleRow(current, car, carSelectQuery,
                        getCarPSConsumer(owner, plate), AssertUtils::assertCar)));


        completableFutures.add(companyMapper
                .findById(new Company.PrimaryKey(companyId, companyCid))
                .thenApply(company -> company.orElse(new Company()))
                .thenAccept(company -> assertSingleRow(current, company, "select id, cid, motto, CAST(version as bigint) version from Company where id = ? and cid = ?",
                        getCompanyPSConsumer(companyId, companyCid), (company1, resultSet) -> assertCompany(company1, resultSet, current))));

        completableFutures.add(topStudentMapper
                .findById(454)
                .thenApply(topStudent -> topStudent.orElse(new TopStudent()))
                .thenAccept(topStudent -> assertSingleRow(current, topStudent, topStudentSelectQuery, getTopStudentPSConsumer(454), AssertUtils::assertTopStudent))
        );

        CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]))
                .join();
    }

    @Test
    public void findAll() {
        UnitOfWork current = UnitOfWork.getCurrent();

        List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
        completableFutures.add(personMapper
                .findAll()
                .thenAccept(people -> assertMultipleRows(current, people, "select nif, name, birthday, CAST(version as bigint) version from Person", AssertUtils::assertPerson, 2)));

        completableFutures.add(carMapper
                .findAll()
                .thenAccept(cars -> assertMultipleRows(current, cars, "select owner, plate, brand, model, CAST(version as bigint) version from Car", AssertUtils::assertCar, 1)));

        CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]))
                .join();
    }

    @Test
    public void create() {
        //Arrange
        Person person = new Person(123, "abc", new Date(1969, 6, 9), 0);
        Car car = new Car(1, "58en60", "Mercedes", "ES1", 0);
        TopStudent topStudent = new TopStudent(456, "Manel", new Date(2020, 12, 1), 0, 1, 20, 2016, 0, 0);

        //Act
        List<CompletableFuture<Boolean>> completableFutures = new ArrayList<>();
        completableFutures.add(personMapper.create(person));
        completableFutures.add(carMapper.create(car));
        completableFutures.add(topStudentMapper.create(topStudent));

        CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]))
                .join();

        //Assert
        assertSingleRow(UnitOfWork.getCurrent(), person, personSelectQuery, getPersonPSConsumer(person.getNif()), AssertUtils::assertPerson);
        assertSingleRow(UnitOfWork.getCurrent(), car, carSelectQuery, getCarPSConsumer(car.getIdentityKey().getOwner(), car.getIdentityKey().getPlate()), AssertUtils::assertCar);
        assertSingleRow(UnitOfWork.getCurrent(), topStudent, topStudentSelectQuery, getTopStudentPSConsumer(topStudent.getNif()), AssertUtils::assertTopStudent);
    }

    @Test
    public void update() throws SQLException {
        ResultSet rs = executeQuery("select CAST(version as bigint) version from Person where nif = ?", getPersonPSConsumer(321));
        Person person = new Person(321, "Maria", new Date(2010, 2, 3), rs.getLong(1));

        rs = executeQuery("select CAST(version as bigint) version from Car where owner = ? and plate = ?", getCarPSConsumer(2, "23we45"));
        Car car = new Car(2, "23we45", "Mitsubishi", "lancer evolution", rs.getLong(1));

        rs = executeQuery("select CAST(P.version as bigint), CAST(S2.version as bigint), CAST(TS.version as bigint) version from Person P " +
                "inner join Student S2 on P.nif = S2.nif " +
                "inner join TopStudent TS on S2.nif = TS.nif where P.nif = ?", getTopStudentPSConsumer(454));
        TopStudent topStudent = new TopStudent(454, "Carlos", new Date(2010, 6, 3), rs.getLong(2),
                4, 6, 7, rs.getLong(3), rs.getLong(1));

        List<CompletableFuture<Boolean>> completableFutures = new ArrayList<>();
        completableFutures.add(personMapper.update(person));
        completableFutures.add(carMapper.update(car));
        completableFutures.add(topStudentMapper.update(topStudent));

        CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]))
                .join();

        assertSingleRow(UnitOfWork.getCurrent(), person, personSelectQuery, getPersonPSConsumer(person.getNif()), AssertUtils::assertPerson);
        assertSingleRow(UnitOfWork.getCurrent(), car, carSelectQuery, getCarPSConsumer(car.getIdentityKey().getOwner(), car.getIdentityKey().getPlate()), AssertUtils::assertCar);
        assertSingleRow(UnitOfWork.getCurrent(), topStudent, topStudentSelectQuery, getTopStudentPSConsumer(topStudent.getNif()), AssertUtils::assertTopStudent);
    }

    @Test
    public void delete() {
        Person person = new Person(321, null, null, 0);
        Car car = new Car(2, "23we45", null, null, 0);
        TopStudent topStudent = new TopStudent(321, null, null, 0, 0, 0, 0, 0, 0);

        List<CompletableFuture<Boolean>> completableFutures = new ArrayList<>();
        completableFutures.add(personMapper.delete(person));
        completableFutures.add(carMapper.delete(car));
        completableFutures.add(topStudentMapper.delete(topStudent));

        CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]))
                .join();

        assertNotFound(personSelectQuery, getPersonPSConsumer(person.getNif()));
        assertNotFound(carSelectQuery, getCarPSConsumer(car.getIdentityKey().getOwner(), car.getIdentityKey().getPlate()));
        assertNotFound(topStudentSelectQuery, getTopStudentPSConsumer(topStudent.getNif()));
    }

}
