package org.github.isel.rapper;

import javafx.util.Pair;
import org.github.isel.rapper.utils.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.sql.Date;
import java.util.*;

import static org.github.isel.rapper.utils.ConnectionManager.DBsPath.TESTDB;
import static org.junit.Assert.*;

/**
 * PersonMapper -> Simple Class with @Id annotation
 * CarMapper -> Simple Class with @EmbeddedId annotation
 * TopStudentMapper -> Class that extends Student and Person and has List of cars annotated with @ColumnName
 */
public class DataMapperTests {

    private DataMapper<Person, Integer> personMapper = MapperRegistry.getMapper(Person.class);
    private DataMapper<Car, Car.PrimaryPk> carMapper = MapperRegistry.getMapper(Car.class);
    private DataMapper<TopStudent, Integer> topStudentMapper = MapperRegistry.getMapper(TopStudent.class);

    @Before
    public void start(){
        MapperRegistry.addEntry(Person.class, new DataMapper<>(Person.class));
        ConnectionManager manager = ConnectionManager.getConnectionManager(TESTDB);
        UnitOfWork.newCurrent(manager::getConnection);
    }

    @After
    public void finish() throws SQLException {
        UnitOfWork.getCurrent().rollback();
        UnitOfWork.getCurrent().closeConnection();
    }

    @Test
    public void findWhere() {
        SqlConsumer<List<Person>> personConsumer = people -> {
            PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select nif, name, birthday, CAST(version as bigint) version from Person where name = ?");
            ps.setString(1, "Jose");
            ResultSet rs = ps.executeQuery();
            while (rs.next())
                assertPerson(people.remove(0), rs);
        };

        SqlConsumer<List<Car>> carConsumer = cars -> {
            PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select owner, plate, brand, model, CAST(version as bigint) version from Car where brand = ?");
            ps.setString(1, "Mitsubishi");
            ResultSet rs = ps.executeQuery();
            while (rs.next())
                assertCar(cars.remove(0), rs);
        };

        personMapper
                .findWhere(new Pair<>("name", "Jose"))
                .thenAccept(personConsumer.wrap());

        carMapper
                .findWhere(new Pair<>("brand", "Mitsubishi"))
                .thenAccept(carConsumer.wrap());
    }

    @Test
    public void getById() {
        int nif = 321;
        int owner = 2;
        String plate = "23we45";


        SqlConsumer<Optional<Person>> personConsumer = optionalPerson -> {
            PreparedStatement ps = UnitOfWork.getCurrent().getConnection()
                    .prepareStatement("select nif, name, birthday, CAST(version as bigint) version from Person where nif = ?");
            ps.setInt(1, nif);
            ResultSet rs = ps.executeQuery();
            if(optionalPerson.isPresent() && rs.next())
                assertPerson(optionalPerson.get(), rs);
            else fail("Person wasn't selected from the database");
        };

        SqlConsumer<Optional<Car>> carConsumer = optionalCar -> {
            PreparedStatement ps = UnitOfWork.getCurrent().getConnection()
                    .prepareStatement("select owner, plate, brand, model, CAST(version as bigint) version from Car where owner = ? and plate = ?");
            ps.setInt(1, owner);
            ps.setString(2, plate);
            ResultSet rs = ps.executeQuery();
            if(optionalCar.isPresent() && rs.next()){
                assertCar(optionalCar.get(), rs);
            }
            else fail("Car wasn't selected from the database");
        };

        personMapper
                .getById(nif)
                .thenAccept(personConsumer.wrap());
        carMapper
                .getById(new Car.PrimaryPk(owner, plate))
                .thenAccept(carConsumer.wrap());
    }

    @Test
    public void getAll() {
        SqlConsumer<List<Person>> personConsumer = people -> {
            PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select nif, name, birthday, CAST(version as bigint) version from Person");
            ResultSet rs = ps.executeQuery();
            if (rs.next())
                assertPerson(people.get(0), rs);
            else fail("People weren't selected from the database");
        };

        SqlConsumer<List<Car>> carConsumer = cars -> {
            PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select owner, plate, brand, model, CAST(version as bigint) version from Car");
            ResultSet rs = ps.executeQuery();
            if(rs.next()){
                assertCar(cars.get(0), rs);
            }
            else fail("Cars weren't selected from the database");
        };

        personMapper
                .getAll()
                .thenAccept(personConsumer.wrap());

        carMapper
                .getAll()
                .thenAccept(carConsumer.wrap());
    }

    private void assertPerson(Person person, ResultSet rs) throws SQLException {
        assertEquals(person.getNif(), rs.getInt("nif"));
        assertEquals(person.getName(), rs.getString("name"));
        assertEquals(person.getBirthday(), rs.getDate("birthday"));
        assertEquals(person.getVersion(), rs.getLong("version"));
        assertNotEquals(0, person.getVersion());
    }

    private void assertCar(Car car, ResultSet rs) throws SQLException {
        assertEquals(car.getIdentityKey().getOwner(), rs.getInt("owner"));
        assertEquals(car.getIdentityKey().getPlate(), rs.getString("plate"));
        assertEquals(car.getBrand(), rs.getString("brand"));
        assertEquals(car.getModel(), rs.getString("model"));
        assertEquals(car.getVersion(), rs.getLong("version"));
        assertNotEquals(0, car.getVersion());
    }

    private void assertTopStudent(TopStudent topStudent, ResultSet rs) throws SQLException {
        assertEquals(topStudent.getNif(), rs.getInt("nif"));
        assertEquals(topStudent.getName(), rs.getString("name"));
        assertEquals(topStudent.getBirthday(), rs.getDate("birthday"));
        assertEquals(topStudent.getVersion(), rs.getLong("version"));
        assertNotEquals(0, topStudent.getVersion());
        assertEquals(topStudent.getTopGrade(), rs.getInt("topGrade"));
        assertEquals(topStudent.getYear(), rs.getInt("year"));
    }

    @Test
    public void insert() throws SQLException {
        //Arrange
        Person person = new Person(123, "abc", new Date(1969, 6, 9), 0);
        Car car = new Car(1, "58en60", "Mercedes", "ES1", 0);
        TopStudent topStudent = new TopStudent(456, "Manel", new Date(2020, 12, 1), 0, 1, 20, 2016, 0, 0);

        //Act
        personMapper.insert(person);
        carMapper.insert(car);
        topStudentMapper.insert(topStudent);

        //Assert
        PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select nif, name, birthday, CAST(version as bigint) version from Person where nif = ?");
        ps.setInt(1, person.getNif());
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            assertPerson(person, rs);
        }
        else fail("Person wasn't inserted in the database");

        ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select owner, plate, brand, model, CAST(version as bigint) version from Car where owner = ? and plate = ?");
        ps.setInt(1, car.getIdentityKey().getOwner());
        ps.setString(2, car.getIdentityKey().getPlate());
        rs = ps.executeQuery();
        if (rs.next()) {
            assertCar(car, rs);
        }
        else fail("Car wasn't inserted in the database");

        ps = UnitOfWork.getCurrent().getConnection()
                .prepareStatement(
                        "select P.nif, P.name, P.birthday, S2.studentNumber, TS.topGrade, TS.year, CAST(TS.version as bigint) version from Person P " +
                        "inner join Student S2 on P.nif = S2.nif " +
                        "inner join TopStudent TS on S2.nif = TS.nif where P.nif = ?"
                );
        ps.setInt(1, topStudent.getNif());
        rs = ps.executeQuery();
        if (rs.next()) {
            assertTopStudent(topStudent, rs);
        }
        else fail("TopStudent wasn't inserted in the database");
    }

    @Test
    public void update() throws SQLException {
        PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select CAST(version as bigint) version from Person where nif = ?");
        ps.setInt(1, 321);
        ResultSet rs = ps.executeQuery();
        rs.next();
        Person person = new Person(321, "Maria", new Date(2010, 2, 3), rs.getLong(1));

        ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select CAST(version as bigint) version from Car where owner = ? and plate = ?");
        ps.setInt(1, 2);
        ps.setString(2, "23we45");
        rs = ps.executeQuery();
        rs.next();
        Car car = new Car(2, "23we45", "Mitsubishi", "lancer evolution", rs.getLong(1));

        ps = UnitOfWork.getCurrent().getConnection().prepareStatement(
                "select CAST(P.version as bigint), CAST(S2.version as bigint), CAST(TS.version as bigint) version from Person P " +
                "inner join Student S2 on P.nif = S2.nif " +
                "inner join TopStudent TS on S2.nif = TS.nif where P.nif = ?"
        );
        ps.setInt(1, 454);
        rs = ps.executeQuery();
        rs.next();
        TopStudent topStudent = new TopStudent(454, "Carlos", new Date(2010, 6, 3), rs.getLong(2),
                4, 6, 7, rs.getLong(3), rs.getLong(1));

        personMapper.update(person);
        carMapper.update(car);
        topStudentMapper.update(topStudent);

        ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select nif, name, birthday, CAST(version as bigint) version from Person where nif = ?");
        ps.setInt(1, person.getNif());
        rs = ps.executeQuery();
        if (rs.next()) {
            assertPerson(person, rs);
        }
        else fail("Person wasn't updated in the database");

        ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select owner, plate, brand, model, CAST(version as bigint) version from Car where owner = ? and plate = ?");
        ps.setInt(1, car.getIdentityKey().getOwner());
        ps.setString(2, car.getIdentityKey().getPlate());
        rs = ps.executeQuery();
        if (rs.next()) {
            assertCar(car, rs);
        }
        else fail("Car wasn't updated in the database");

        ps = UnitOfWork.getCurrent().getConnection().prepareStatement(
                "select P.nif, P.name, P.birthday, S2.studentNumber, TS.topGrade, TS.year, CAST(TS.version as bigint) version from Person P " +
                        "inner join Student S2 on P.nif = S2.nif " +
                        "inner join TopStudent TS on S2.nif = TS.nif where P.nif = ?"
        );
        ps.setInt(1, topStudent.getNif());
        rs = ps.executeQuery();
        if (rs.next()) {
            assertTopStudent(topStudent, rs);
        }
        else fail("TopStudent wasn't updated in the database");
    }

    @Test
    public void delete() throws SQLException {
        Person person = new Person(321, null, null, 0);
        Car car = new Car(2, "23we45", null, null, 0);
        TopStudent topStudent = new TopStudent(321, null, null, 0, 0, 0, 0, 0, 0);

        personMapper.delete(person);
        carMapper.delete(car);
        topStudentMapper.delete(topStudent);

        PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select nif, name, birthday, CAST(version as bigint) version from Person where nif = ?");
        ps.setInt(1, person.getNif());
        ResultSet rs = ps.executeQuery();
        assertFalse(rs.next());

        ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select owner, plate, brand, model, CAST(version as bigint) version from Car where owner = ? and plate = ?");
        ps.setInt(1, car.getIdentityKey().getOwner());
        ps.setString(2, car.getIdentityKey().getPlate());
        rs = ps.executeQuery();
        assertFalse(rs.next());

        ps = UnitOfWork.getCurrent().getConnection().prepareStatement(
                "select P.nif, P.name, P.birthday, S2.studentNumber, TS.topGrade, TS.year, CAST(P.version as bigint) version from Person P " +
                        "inner join Student S2 on P.nif = S2.nif " +
                        "inner join TopStudent TS on S2.nif = TS.nif where P.nif = ?"
        );
        ps.setInt(1, topStudent.getNif());
        rs = ps.executeQuery();
        assertFalse(rs.next());
    }
}
