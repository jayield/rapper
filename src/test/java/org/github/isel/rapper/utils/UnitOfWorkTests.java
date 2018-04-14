package org.github.isel.rapper.utils;

import org.github.isel.rapper.AssertUtils;
import org.github.isel.rapper.DataRepository;
import org.github.isel.rapper.domainModel.Car;
import org.github.isel.rapper.DomainObject;
import org.github.isel.rapper.domainModel.Employee;
import org.github.isel.rapper.domainModel.Person;
import org.github.isel.rapper.domainModel.TopStudent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

import static junit.framework.TestCase.assertTrue;
import static org.github.isel.rapper.TestUtils.*;
import static org.github.isel.rapper.utils.ConnectionManager.DBsPath.TESTDB;
import static org.github.isel.rapper.utils.MapperRegistry.getRepository;

public class UnitOfWorkTests {

    private Container container;
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
        UnitOfWork.newCurrent(manager::getConnection);
        UnitOfWork.getCurrent().getConnection().prepareCall("{call deleteDB}").execute();
        UnitOfWork.getCurrent().getConnection().prepareCall("{call populateDB}").execute();

        container = new Container();

        setupLists();
    }

    @After
    public void finish() throws SQLException {
        //UnitOfWork.getCurrent().rollback();
        UnitOfWork.getCurrent().getConnection().prepareCall("{call deleteDB}").execute();
        UnitOfWork.getCurrent().closeConnection();
    }

    /**
     * Obter as listas do Unit
     * Popular
     * Chamar Commit
     * Verificar IdentiyMaps
     * Verificar Base de dados
     */
    @Test
    public void commit() throws NoSuchFieldException, IllegalAccessException {
        populateIdentityMaps();
        addNewObjects();
        addDirtyAndClonedObjects();
        addRemovedObjects();

        assertTrue(UnitOfWork.getCurrent().commit().join());

        Field identityMapField = DataRepository.class.getDeclaredField("identityMap");

        assertIdentityMaps(identityMapField, newObjects, (identityMap, domainObject) -> assertTrue(identityMap.containsValue(domainObject)));
        assertIdentityMaps(identityMapField, dirtyObjects, (identityMap, domainObject) -> assertTrue(identityMap.containsValue(domainObject)));
        assertIdentityMaps(identityMapField, removedObjects, (identityMap, domainObject) -> assertTrue(!identityMap.containsValue(domainObject)));

        assertNewObjects();
        assertDirtyObjects();
        assertRemovedObjects();
    }

    @Test
    public void rollback() {
    }

    private void assertIdentityMaps(Field identityMapField, List<DomainObject> dirtyObjects, BiConsumer<ConcurrentMap, DomainObject> assertion) throws IllegalAccessException {
        for (DomainObject domainObject : dirtyObjects) {
            DataRepository repository = getRepository(domainObject.getClass());
            ConcurrentMap identityMap = (ConcurrentMap) identityMapField.get(repository);
            assertion.accept(identityMap, domainObject);
        }
    }

    private void assertRemovedObjects() {
        Employee originalEmployee = container.getOriginalEmployee();
        assertDelete(employeeSelectQuery, getEmployeePSConsumer(originalEmployee.getName()));
    }

    private void assertDirtyObjects() {
        Person person = container.getUpdatedPerson();
        Car car = container.getUpdatedCar();
        TopStudent topStudent = container.getUpdatedTopStudent();

        assertSingleRow(UnitOfWork.getCurrent(), person, personSelectQuery, getPersonPSConsumer(person.getNif()), AssertUtils::assertPerson);
        assertSingleRow(UnitOfWork.getCurrent(), car, carSelectQuery, getCarPSConsumer(car.getIdentityKey().getOwner(), car.getIdentityKey().getPlate()), AssertUtils::assertCar);
        assertSingleRow(UnitOfWork.getCurrent(), topStudent, topStudentSelectQuery, getTopStudentPSConsumer(topStudent.getNif()), AssertUtils::assertTopStudent);
    }

    private void assertNewObjects() {
        Person person = container.getInsertedPerson();
        Car car = container.getInsertedCar();
        TopStudent topStudent = container.getInsertedTopStudent();

        assertSingleRow(UnitOfWork.getCurrent(), person, personSelectQuery, getPersonPSConsumer(person.getNif()), AssertUtils::assertPerson);
        assertSingleRow(UnitOfWork.getCurrent(), car, carSelectQuery, getCarPSConsumer(car.getIdentityKey().getOwner(), car.getIdentityKey().getPlate()), AssertUtils::assertCar);
        assertSingleRow(UnitOfWork.getCurrent(), topStudent, topStudentSelectQuery, getTopStudentPSConsumer(topStudent.getNif()), AssertUtils::assertTopStudent);
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
        private Person insertedPerson;
        private Car insertedCar;
        private TopStudent insertedTopStudent;
        private Person updatedPerson;
        private Car updatedCar;
        private TopStudent updatedTopStudent;
        private Car originalCar;
        private Employee originalEmployee;

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
                    "inner join TopStudent TS on S2.nif = TS.nif where P.nif = ?", getTopStudentPSConsumer(454));
            updatedTopStudent = new TopStudent(454, "Carlos", new Date(2010, 6, 3), rs.getLong(2),
                    4, 6, 7, rs.getLong(3), rs.getLong(1));

            rs = executeQuery(carSelectQuery, getCarPSConsumer(2, "23we45"));
            originalCar = new Car(rs.getInt("owner"), rs.getString("plate"), rs.getString("brand"), rs.getString("model"), rs.getLong("version"));

            rs = executeQuery(employeeSelectQuery, getEmployeePSConsumer("Charles"));
            originalEmployee = new Employee(rs.getInt("id"), rs.getString("name"), rs.getInt("companyId"), rs.getInt("companyCid"),
                    rs.getLong("version"), null);
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