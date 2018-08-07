package com.github.jayield.rapper.unitofwork;

import com.github.jayield.rapper.*;
import com.github.jayield.rapper.connections.ConnectionManager;
import com.github.jayield.rapper.domainModel.*;
import com.github.jayield.rapper.mapper.DataMapper;
import com.github.jayield.rapper.mapper.MapperRegistry;
import com.github.jayield.rapper.sql.SqlSupplier;
import com.github.jayield.rapper.AssertUtils;
import com.github.jayield.rapper.utils.EqualCondition;
import com.github.jayield.rapper.utils.Pair;
import com.github.jayield.rapper.utils.SqlUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.TransactionIsolation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.net.URLDecoder;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

import static com.github.jayield.rapper.AssertUtils.*;
import static com.github.jayield.rapper.TestUtils.*;
import static com.github.jayield.rapper.mapper.MapperRegistry.getMapper;
import static org.junit.Assert.*;

public class UnitOfWorkTests {

    private ObjectsContainer objectsContainer;
    private final Logger logger = LoggerFactory.getLogger(UnitOfWorkTests.class);
    private DataMapper<Employee, Integer> employeeRepo;
    private DataMapper<Company, Company.PrimaryKey> companyRepo;
    private Queue<DomainObject> newObjects;
    private Queue<DomainObject> dirtyObjects;
    private Queue<DomainObject> removedObjects;
    private Map<Class, MapperRegistry.Container> containerMap;
    private UnitOfWork unit;

    public UnitOfWorkTests() throws NoSuchFieldException, IllegalAccessException {
        Field repositoryMapField = MapperRegistry.class.getDeclaredField("containerMap");
        repositoryMapField.setAccessible(true);
        containerMap = (Map<Class, MapperRegistry.Container>) repositoryMapField.get(null);
    }

    private void setupLists() throws NoSuchFieldException, IllegalAccessException {
        if(newObjects == null || dirtyObjects == null || removedObjects == null) {
            Field newObjectsField = UnitOfWork.class.getDeclaredField("newObjects");
            newObjectsField.setAccessible(true);

            Field dirtyObjectsField = UnitOfWork.class.getDeclaredField("dirtyObjects");
            dirtyObjectsField.setAccessible(true);

            Field removedObjectsField = UnitOfWork.class.getDeclaredField("removedObjects");
            removedObjectsField.setAccessible(true);

            this.newObjects = (Queue<DomainObject>) newObjectsField.get(unit);
            this.dirtyObjects = (Queue<DomainObject>) dirtyObjectsField.get(unit);
            this.removedObjects = (Queue<DomainObject>) removedObjectsField.get(unit);
        }
    }

    @Before
    public void before()throws NoSuchFieldException, IllegalAccessException {
        //UnitOfWork.connectionsMap.values().forEach(array -> System.out.println(Arrays.toString(array)));
        assertEquals(0, UnitOfWork.getNumberOfOpenConnections().get());
        containerMap.clear();

        ConnectionManager manager = ConnectionManager.getConnectionManager(
                "jdbc:hsqldb:file:" + URLDecoder.decode(this.getClass().getClassLoader().getResource("testdb").getPath()) + "/testdb",
                "SA",
                ""
        );

        unit = new UnitOfWork(manager::getConnection);

        SQLConnection con = unit.getConnection().join();
        SqlUtils.<ResultSet>callbackToPromise(ar -> con.call("{call deleteDB()}", ar)).join();
        SqlUtils.<ResultSet>callbackToPromise(ar -> con.call("{call populateDB()}", ar)).join();

        objectsContainer = new ObjectsContainer(con);
        unit.commit().join();
        setupLists();
    }

    @After
    public void after() {
        assertEquals(0, UnitOfWork.getNumberOfOpenConnections().get());
    }

    @Test
    public void testCommit() {
        populateIdentityMaps();
        addNewObjects();
        addDirtyObjects();
        addRemovedObjects();

        List<DomainObject> newObjects = new ArrayList<>(this.newObjects);
        List<DomainObject> dirtyObjects = new ArrayList<>(this.dirtyObjects);
        List<DomainObject> removedObjects = new ArrayList<>(this.removedObjects);

        unit.commit().join();

        assertNewObjects(true);
        assertDirtyObjects(true);
        assertRemovedObjects(true);

        /*assertIdentityMaps(newObjects, (identityMap, domainObject) -> assertEquals(((CompletableFuture<DomainObject>) identityMap.get(domainObject.getIdentityKey())).join(), domainObject));
        assertIdentityMaps(dirtyObjects, (identityMap, domainObject) -> assertEquals(((CompletableFuture<DomainObject>) identityMap.get(domainObject.getIdentityKey())).join(), domainObject));
        assertIdentityMaps(removedObjects, (identityMap, domainObject) -> assertNull(identityMap.get(domainObject.getIdentityKey())));*/
    }

    @Test
    public void testRollback() {
        populateIdentityMaps();
        addNewObjects();
        addDirtyObjects();
        addRemovedObjects();

        List<DomainObject> newObjects = new ArrayList<>(this.newObjects);
        List<DomainObject> dirtyObjects = new ArrayList<>(this.dirtyObjects);
        List<DomainObject> removedObjects = new ArrayList<>(this.removedObjects);

        unit.rollback().join();

        assertNewObjects(false);
        assertDirtyObjects(false);
        assertRemovedObjects(false);

        assertIdentityMaps(newObjects, (identityMap, domainObject) -> assertNull(identityMap.get(domainObject.getIdentityKey())));
        assertIdentityMaps(dirtyObjects, (identityMap, domainObject) -> {
            CompletableFuture<DomainObject> completableFuture = (CompletableFuture<DomainObject>) identityMap.get(domainObject.getIdentityKey());
            if(completableFuture != null)
                assertNotEquals(completableFuture.join(), domainObject);
            else
                assertNull(identityMap.get(domainObject.getIdentityKey()));
        });
        assertIdentityMaps(removedObjects, (identityMap, domainObject) -> assertEquals(((CompletableFuture<DomainObject>) identityMap.get(domainObject.getIdentityKey())).join(), domainObject));
    }

    @Test
    public void testTransaction() {
        logger.info("Number of Openned connections - {}", UnitOfWork.getNumberOfOpenConnections().get());
        ConnectionManager connectionManager = ConnectionManager.getConnectionManager();
        SqlSupplier<CompletableFuture<SQLConnection>> connectionSqlSupplier = () -> connectionManager.getConnection(TransactionIsolation.READ_UNCOMMITTED.getType());

        employeeRepo = MapperRegistry.getMapper(Employee.class, unit);

        Employee employee = employeeRepo.find(new EqualCondition<>("name", "Bob")).join().get(0);
        Employee employee2 = employeeRepo.find(new EqualCondition<>("name", "Charles")).join().get(0);
        unit.commit().join();

        UnitOfWork unitOfWork = new UnitOfWork(connectionSqlSupplier.wrap());

        DataMapper<Employee, Integer> employeeRepo2 = MapperRegistry.getMapper(Employee.class, unitOfWork);
        CompletableFuture<Void> future1 = employeeRepo2.deleteById(employee.getIdentityKey());
        CompletableFuture<Void> future = employeeRepo2.deleteById(employee2.getIdentityKey());

        companyRepo = MapperRegistry.getMapper(Company.class, unitOfWork);

        CompletableFuture.allOf(future, future1)
                .thenCompose(aVoid -> companyRepo.deleteById(new Company.PrimaryKey(1, 1)))
                .thenCompose(voidCompletableFuture -> unitOfWork.commit())
                .join();

        assertTrue(employeeRepo.find(new EqualCondition<>("name", "Bob")).join().isEmpty());
        assertTrue(employeeRepo.find(new EqualCondition<>("name", "Charles")).join().isEmpty());
        assertTrue(!companyRepo.findById(new Company.PrimaryKey(1, 1)).join().isPresent());
        unit.rollback().join();
        unitOfWork.rollback().join();
    }

    private void assertIdentityMaps(List<DomainObject> objectList, BiConsumer<ConcurrentMap, DomainObject> assertion) {
        for (DomainObject domainObject : objectList) {
            ConcurrentMap identityMap = unit.getIdentityMap(domainObject.getClass());
            assertion.accept(identityMap, domainObject);
        }
    }

    private void assertRemovedObjects(boolean isCommit) {
        Employee originalEmployee = objectsContainer.getOriginalEmployee();
        UnitOfWork unit = new UnitOfWork();
        SQLConnection con = unit.getConnection().join();
        if(isCommit) {
            assertNotFound(employeeSelectQuery, new JsonArray().add(originalEmployee.getName()), con);
        }
        else{
            assertSingleRow(originalEmployee, employeeSelectQuery, new JsonArray().add(originalEmployee.getName()), (employee, rs) -> AssertUtils.assertEmployeeWithExternals(employee, rs, unit), con);
        }
        unit.rollback().join();
    }

    private void assertDirtyObjects(boolean isCommit) {
        Person person;
        Car car;
        TopStudent topStudent;
        if(isCommit) {
            person = objectsContainer.getUpdatedPerson();
            car = objectsContainer.getUpdatedCar();
            topStudent = objectsContainer.getUpdatedTopStudent();
        }
        else {
            person = objectsContainer.getOriginalPerson();
            car = objectsContainer.getOriginalCar();
            topStudent = objectsContainer.getOriginalTopStudent();
        }

        UnitOfWork unit = new UnitOfWork();
        SQLConnection con = unit.getConnection().join();

        assertSingleRow(person, personSelectQuery, new JsonArray().add(person.getNif()), AssertUtils::assertPerson, con);
        assertSingleRow(car, carSelectQuery, new JsonArray().add(car.getIdentityKey().getOwner()).add(car.getIdentityKey().getPlate()), AssertUtils::assertCar, con);
        assertSingleRow(topStudent, topStudentSelectQuery, new JsonArray().add(topStudent.getNif()), AssertUtils::assertTopStudent, con);

        unit.rollback().join();
    }

    private void assertNewObjects(boolean isCommit) {
        Person person = objectsContainer.getInsertedPerson();
        Car car = objectsContainer.getInsertedCar();
        TopStudent topStudent = objectsContainer.getInsertedTopStudent();

        UnitOfWork unit = new UnitOfWork();
        SQLConnection con = unit.getConnection().join();

        if(isCommit) {
            assertSingleRow(person, personSelectQuery, new JsonArray().add(person.getNif()), AssertUtils::assertPerson, con);
            assertSingleRow(car, carSelectQuery, new JsonArray().add(car.getIdentityKey().getOwner()).add(car.getIdentityKey().getPlate()), AssertUtils::assertCar, con);
            assertSingleRow(topStudent, topStudentSelectQuery, new JsonArray().add(topStudent.getNif()), AssertUtils::assertTopStudent, con);
        }
        else {
            assertNotFound(personSelectQuery, new JsonArray().add(person.getNif()), con);
            assertNotFound(carSelectQuery, new JsonArray().add(car.getIdentityKey().getOwner()).add(car.getIdentityKey().getPlate()), con);
            assertNotFound(topStudentSelectQuery, new JsonArray().add(topStudent.getNif()), con);
        }

        unit.rollback().join();
    }

    private void populateIdentityMaps() {
        Car originalCar = objectsContainer.getOriginalCar();
        unit.validate(originalCar.getIdentityKey(), originalCar);

        Employee originalEmployee = objectsContainer.getOriginalEmployee();
        unit.validate(originalEmployee.getIdentityKey(), originalEmployee);
    }

    private void addRemovedObjects() {
        getMapper(Employee.class, unit).delete(objectsContainer.getOriginalEmployee()).join();
    }

    //Original car shall be in the identityMap
    private void addDirtyObjects() {
        getMapper(Person.class, unit).update(objectsContainer.getUpdatedPerson()).join();
        getMapper(Car.class, unit).update(objectsContainer.getUpdatedCar()).join();
        getMapper(TopStudent.class, unit).update(objectsContainer.getUpdatedTopStudent()).join();
    }

    private void addNewObjects() {
        getMapper(Person.class, unit).create(objectsContainer.getInsertedPerson()).join();
        getMapper(Car.class, unit).create(objectsContainer.getInsertedCar()).join();
        getMapper(TopStudent.class, unit).create(objectsContainer.getInsertedTopStudent()).join();
    }
}