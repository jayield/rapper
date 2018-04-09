package org.github.isel.rapper;

import javafx.util.Pair;
import org.github.isel.rapper.utils.ConnectionManager;
import org.github.isel.rapper.utils.MapperRegistry;
import org.github.isel.rapper.utils.SqlConsumer;
import org.github.isel.rapper.utils.UnitOfWork;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

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
    public void findWhere() throws SQLException {
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
    public void getById() throws SQLException {
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
    }

    private void assertCar(Car car, ResultSet rs) throws SQLException {
        assertEquals(car.getIdentityKey().getOwner(), rs.getInt("owner"));
        assertEquals(car.getIdentityKey().getPlate(), rs.getString("plate"));
        assertEquals(car.getBrand(), rs.getString("brand"));
        assertEquals(car.getModel(), rs.getString("model"));
        assertEquals(car.getVersion(), rs.getLong("version"));
    }

    @Test
    public void insert() throws SQLException {
        //Arrange
        Person person = new Person(123, "abc", new Date(1969, 6, 9), 0);
        Car car = new Car(1, "58en60", "Mercedes", "ES1", 0);

        //Act
        personMapper.insert(person);
        carMapper.insert(car);

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
    }

    @Test
    public void update() throws SQLException {
        Person person = new Person(321, "Maria", new Date(2010, 2, 3), 0);
        Car car = new Car(2, "23we45", "Mitsubishi", "lancer evolution", 0);

        personMapper.update(person);
        carMapper.update(car);

        PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select nif, name, birthday, CAST(version as bigint) version from Person where nif = ?");
        ps.setInt(1, person.getNif());
        ResultSet rs = ps.executeQuery();
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
    }

    @Test
    public void delete() throws SQLException {
        Person person = new Person(321, null, null, 0);
        Car car = new Car(2, "23we45", null, null, 0);

        personMapper.delete(person);
        carMapper.delete(car);

        PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select nif, name, birthday, CAST(version as bigint) version from Person where nif = ?");
        ps.setInt(1, person.getNif());
        ResultSet rs = ps.executeQuery();
        assertFalse(rs.next());

        ps = UnitOfWork.getCurrent().getConnection().prepareStatement("select owner, plate, brand, model, CAST(version as bigint) version from Car where owner = ? and plate = ?");
        ps.setInt(1, car.getIdentityKey().getOwner());
        ps.setString(2, car.getIdentityKey().getPlate());
        rs = ps.executeQuery();
        assertFalse(rs.next());
    }

    @Test
    public void getSelectQuery() {
        assertEquals("select nif, name, birthday, CAST(version as bigint) version from Person", personMapper.getSelectQuery());
        assertEquals("select owner, plate, brand, model, CAST(version as bigint) version from Car", carMapper.getSelectQuery());
    }

    @Test
    public void getInsertQuery() {
        assertEquals("insert into Person ( nif, name, birthday ) output CAST(INSERTED.version as bigint) version values ( ?, ?, ? )", personMapper.getInsertQuery());
        assertEquals("insert into Car ( owner, plate, brand, model ) output CAST(INSERTED.version as bigint) version values ( ?, ?, ?, ? )", carMapper.getInsertQuery());
    }

    @Test
    public void getUpdateQuery() {
        assertEquals("update Person set nif = ?, name = ?, birthday = ? output CAST(INSERTED.version as bigint) version where nif = ?", personMapper.getUpdateQuery());
        assertEquals("update Car set owner = ?, plate = ?, brand = ?, model = ? output CAST(INSERTED.version as bigint) version where owner = ? and plate = ?", carMapper.getUpdateQuery());
    }

    @Test
    public void getDeleteQuery() {
        assertEquals("delete from Person where nif = ?", personMapper.getDeleteQuery());
        assertEquals("delete from Car where owner = ? and plate = ?", carMapper.getDeleteQuery());
    }
}
