package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.DataRepository;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.TestUtils;
import com.github.jayield.rapper.domainModel.*;
import com.github.jayield.rapper.AssertUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

import static com.github.jayield.rapper.AssertUtils.*;
import static com.github.jayield.rapper.TestUtils.*;
import static com.github.jayield.rapper.utils.DBsPath.TESTDB;
import static com.github.jayield.rapper.utils.MapperRegistry.getRepository;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UnitOfWorkTests {

    private Container container;
    private final Logger logger = LoggerFactory.getLogger(UnitOfWorkTests.class);
    private List<DomainObject> newObjects;
    private List<DomainObject> clonedObjects;
    private List<DomainObject> dirtyObjects;
    private List<DomainObject> removedObjects;

    private void setupLists() throws NoSuchFieldException, IllegalAccessException {
        if(newObjects == null || clonedObjects == null || dirtyObjects == null || removedObjects == null) {
            Field newObjectsField = UnitOfWork.class.getDeclaredField("newObjects");
            newObjectsField.setAccessible(true);

            Field clonedObjectsField = UnitOfWork.class.getDeclaredField("clonedObjects");
            clonedObjectsField.setAccessible(true);

            Field dirtyObjectsField = UnitOfWork.class.getDeclaredField("dirtyObjects");
            dirtyObjectsField.setAccessible(true);

            Field removedObjectsField = UnitOfWork.class.getDeclaredField("removedObjects");
            removedObjectsField.setAccessible(true);

            this.newObjects = (List<DomainObject>) newObjectsField.get(UnitOfWork.getCurrent());
            this.clonedObjects = (List<DomainObject>) clonedObjectsField.get(UnitOfWork.getCurrent());
            this.dirtyObjects = (List<DomainObject>) dirtyObjectsField.get(UnitOfWork.getCurrent());
            this.removedObjects = (List<DomainObject>) removedObjectsField.get(UnitOfWork.getCurrent());
        }
    }

    @Before
    public void before() throws SQLException, NoSuchFieldException, IllegalAccessException {
        ConnectionManager manager = ConnectionManager.getConnectionManager(TESTDB);
        SqlSupplier<Connection> connectionSupplier = manager::getConnection;
        UnitOfWork.newCurrent(connectionSupplier.wrap());
        Connection con = UnitOfWork.getCurrent().getConnection();
        con.prepareCall("{call deleteDB}").execute();
        con.prepareCall("{call populateDB}").execute();
        con.commit();
        /*createTables(con);
        deleteDB(con);
        populateDB(con);*/

        container = new Container();

        setupLists();
    }

    @After
    public void finish() {
        UnitOfWork.getCurrent().closeConnection();
    }

    @Test
    public void commit() throws NoSuchFieldException, IllegalAccessException {
        populateIdentityMaps();
        addNewObjects();
        addDirtyAndClonedObjects();
        addRemovedObjects();

        List<DomainObject> newObjects = new ArrayList<>(this.newObjects);
        List<DomainObject> dirtyObjects = new ArrayList<>(this.dirtyObjects);
        List<DomainObject> removedObjects = new ArrayList<>(this.removedObjects);

        assertTrue(UnitOfWork.getCurrent().commit().join());

        Field identityMapField = DataRepository.class.getDeclaredField("identityMap");
        identityMapField.setAccessible(true);

        assertIdentityMaps(identityMapField, newObjects, (identityMap, domainObject) -> assertTrue(identityMap.containsValue(domainObject)));
        assertIdentityMaps(identityMapField, dirtyObjects, (identityMap, domainObject) -> assertTrue(identityMap.containsValue(domainObject)));
        assertIdentityMaps(identityMapField, removedObjects, (identityMap, domainObject) -> assertTrue(!identityMap.containsValue(domainObject)));

        assertNewObjects(true);
        assertDirtyObjects(true);
        assertRemovedObjects(true);
    }

    @Test
    public void rollback() throws IllegalAccessException, NoSuchFieldException {
        populateIdentityMaps();
        addNewObjects();
        addDirtyAndClonedObjects();
        addRemovedObjects();

        //There's no table Chat in DB, so there will be a SQLException and a rollback
        Chat chat = new Chat();
        removedObjects.add(chat);

        List<DomainObject> newObjects = new ArrayList<>(this.newObjects);
        List<DomainObject> dirtyObjects = new ArrayList<>(this.dirtyObjects);
        List<DomainObject> removedObjects = new ArrayList<>(this.removedObjects);

        assertFalse(UnitOfWork.getCurrent().commit().join());

        removedObjects.remove(chat);
        Field identityMapField = DataRepository.class.getDeclaredField("identityMap");
        identityMapField.setAccessible(true);

        assertIdentityMaps(identityMapField, newObjects, (identityMap, domainObject) -> assertTrue(!identityMap.containsValue(domainObject)));
        assertIdentityMaps(identityMapField, dirtyObjects, (identityMap, domainObject) -> assertTrue(!identityMap.containsValue(domainObject)));
        assertIdentityMaps(identityMapField, removedObjects, (identityMap, domainObject) -> assertTrue(identityMap.containsValue(domainObject)));

        assertNewObjects(false);
        assertDirtyObjects(false);
        assertRemovedObjects(false);
    }

    private void assertIdentityMaps(Field identityMapField, List<DomainObject> dirtyObjects, BiConsumer<ConcurrentMap, DomainObject> assertion) throws IllegalAccessException {
        for (DomainObject domainObject : dirtyObjects) {
            DataRepository repository = getRepository(domainObject.getClass());
            ConcurrentMap identityMap = (ConcurrentMap) identityMapField.get(repository);
            assertion.accept(identityMap, domainObject);
        }
    }

    private void assertRemovedObjects(boolean isCommit) {
        Employee originalEmployee = container.getOriginalEmployee();

        if(isCommit) {
            assertNotFound(employeeSelectQuery, getEmployeePSConsumer(originalEmployee.getName()));
        }
        else{
            assertSingleRow(originalEmployee, employeeSelectQuery, getEmployeePSConsumer(originalEmployee.getName()), AssertUtils::assertEmployee);
        }
    }

    private void assertDirtyObjects(boolean isCommit) {
        Person person = null;
        Car car = null;
        TopStudent topStudent = null;
        if(isCommit) {
            person = container.getUpdatedPerson();
            car = container.getUpdatedCar();
            topStudent = container.getUpdatedTopStudent();
        }
        else {
            person = container.getOriginalPerson();
            car = container.getOriginalCar();
            topStudent = container.getOriginalTopStudent();
        }

        assertSingleRow(person, personSelectQuery, getPersonPSConsumer(person.getNif()), AssertUtils::assertPerson);
        assertSingleRow(car, carSelectQuery, getCarPSConsumer(car.getIdentityKey().getOwner(), car.getIdentityKey().getPlate()), AssertUtils::assertCar);
        assertSingleRow(topStudent, topStudentSelectQuery, getPersonPSConsumer(topStudent.getNif()), AssertUtils::assertTopStudent);
    }

    private void assertNewObjects(boolean isCommit) {
        Person person = container.getInsertedPerson();
        Car car = container.getInsertedCar();
        TopStudent topStudent = container.getInsertedTopStudent();

        if(isCommit) {
            assertSingleRow(person, personSelectQuery, getPersonPSConsumer(person.getNif()), AssertUtils::assertPerson);
            assertSingleRow(car, carSelectQuery, getCarPSConsumer(car.getIdentityKey().getOwner(), car.getIdentityKey().getPlate()), AssertUtils::assertCar);
            assertSingleRow(topStudent, topStudentSelectQuery, getPersonPSConsumer(topStudent.getNif()), AssertUtils::assertTopStudent);
        }
        else {
            assertNotFound(personSelectQuery, getPersonPSConsumer(person.getNif()));
            assertNotFound(carSelectQuery, getCarPSConsumer(car.getIdentityKey().getOwner(), car.getIdentityKey().getPlate()));
            assertNotFound(topStudentSelectQuery, getPersonPSConsumer(topStudent.getNif()));
        }
    }

    private void populateIdentityMaps() {
        DataRepository carRepo = getRepository(Car.class);
        Car originalCar = container.getOriginalCar();
        carRepo.validate(originalCar.getIdentityKey(), originalCar);

        DataRepository employeeRepo = getRepository(Employee.class);
        Employee originalEmployee = container.getOriginalEmployee();
        employeeRepo.validate(originalEmployee.getIdentityKey(), originalEmployee);
    }

    private void addRemovedObjects() {
        removedObjects.add(container.getOriginalEmployee());
    }

    //Original car shall be in the identityMap
    private void addDirtyAndClonedObjects() {
        clonedObjects.add(container.getOriginalCar());

        dirtyObjects.add(container.getUpdatedPerson());
        dirtyObjects.add(container.getUpdatedCar());
        dirtyObjects.add(container.getUpdatedTopStudent());
    }

    private void addNewObjects() {
        newObjects.add(container.getInsertedPerson());
        newObjects.add(container.getInsertedCar());
        newObjects.add(container.getInsertedTopStudent());
    }

    private class Container {
        private final Person originalPerson;
        private final Person insertedPerson;
        private final Car insertedCar;
        private final TopStudent insertedTopStudent;
        private final Person updatedPerson;
        private final Car updatedCar;
        private final TopStudent updatedTopStudent;
        private final Car originalCar;
        private final Employee originalEmployee;
        private final TopStudent originalTopStudent;

        public Container() throws SQLException {
            insertedPerson = new Person(123, "abc", new Date(1969, 6, 9), 0);
            insertedCar = new Car(1, "58en60", "Mercedes", "ES1", 0);
            insertedTopStudent = new TopStudent(456, "Manel", new Date(2020, 12, 1), 0, 1, 20, 2016, 0, 0);

            ResultSet rs = executeQuery("select CAST(version as bigint) version from Person where nif = ?", getPersonPSConsumer(321));
            updatedPerson = new Person(321, "Maria", new Date(2010, 2, 3), rs.getLong(1));

            rs = executeQuery("select CAST(version as bigint) version from Car where owner = ? and plate = ?", getCarPSConsumer(2, "23we45"));
            updatedCar = new Car(2, "23we45", "Mitsubishi", "lancer evolution", rs.getLong(1));

            rs = executeQuery("select CAST(P.version as bigint), CAST(S2.version as bigint), CAST(TS.version as bigint) version from Person P " +
                    "inner join Student S2 on P.nif = S2.nif " +
                    "inner join TopStudent TS on S2.nif = TS.nif where P.nif = ?", getPersonPSConsumer(454));
            updatedTopStudent = new TopStudent(454, "Carlos", new Date(2010, 6, 3), rs.getLong(2),
                    4, 6, 7, rs.getLong(3), rs.getLong(1));

            rs = executeQuery(personSelectQuery, getPersonPSConsumer(321));
            originalPerson = new Person(rs.getInt("nif"), rs.getString("name"), rs.getDate("birthday"), rs.getLong("version"));

            rs = executeQuery(carSelectQuery, getCarPSConsumer(2, "23we45"));
            originalCar = new Car(rs.getInt("owner"), rs.getString("plate"), rs.getString("brand"), rs.getString("model"), rs.getLong("version"));

            rs = executeQuery(topStudentSelectQuery, getPersonPSConsumer(454));
            originalTopStudent = new TopStudent(rs.getInt("nif"), rs.getString("name"), rs.getDate("birthday"), rs.getLong("P1version"), rs.getInt("studentNumber"),
                    rs.getInt("topGrade"), rs.getInt("year"), rs.getLong("Cversion"), rs.getLong("P2version"));

            rs = executeQuery(companySelectQuery, getCompanyPSConsumer(1, 1));
            Company originalCompany = new Company(new Company.PrimaryKey(rs.getInt("id"), rs.getInt("cid")), rs.getString("motto"), null, rs.getLong("Cversion"));

            rs = executeQuery(employeeSelectQuery, getEmployeePSConsumer("Charles"));
            originalEmployee = new Employee(rs.getInt("id"), rs.getString("name"), rs.getLong("version"), CompletableFuture.completedFuture(originalCompany));
        }

        public TopStudent getOriginalTopStudent() {
            return originalTopStudent;
        }

        public Person getOriginalPerson() {
            return originalPerson;
        }

        public Person getInsertedPerson() {
            return insertedPerson;
        }

        public Car getInsertedCar() {
            return insertedCar;
        }

        public TopStudent getInsertedTopStudent() {
            return insertedTopStudent;
        }

        public Person getUpdatedPerson() {
            return updatedPerson;
        }

        public Car getUpdatedCar() {
            return updatedCar;
        }

        public TopStudent getUpdatedTopStudent() {
            return updatedTopStudent;
        }

        public Car getOriginalCar() {
            return originalCar;
        }

        public Employee getOriginalEmployee() {
            return originalEmployee;
        }
    }
}