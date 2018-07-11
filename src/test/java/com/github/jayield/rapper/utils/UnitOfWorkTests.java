package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.*;
import com.github.jayield.rapper.domainModel.*;
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
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URLDecoder;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

import static com.github.jayield.rapper.AssertUtils.*;
import static com.github.jayield.rapper.TestUtils.*;
import static com.github.jayield.rapper.utils.MapperRegistry.getRepository;
import static org.junit.Assert.*;

public class UnitOfWorkTests {

    private ObjectsContainer objectsContainer;
    private final Logger logger = LoggerFactory.getLogger(UnitOfWorkTests.class);
    private DataRepository<Employee, Integer> employeeRepo;
    private DataRepository<Company, Company.PrimaryKey> companyRepo;
    private Queue<DomainObject> newObjects;
    private Queue<DomainObject> clonedObjects;
    private Queue<DomainObject> dirtyObjects;
    private Queue<DomainObject> removedObjects;
    private Map<Class, DataRepository> repositoryMap;
    private UnitOfWork unit;

    public UnitOfWorkTests() throws NoSuchFieldException, IllegalAccessException {
        Field repositoryMapField = MapperRegistry.class.getDeclaredField("containerMap");
        repositoryMapField.setAccessible(true);
        repositoryMap = (Map<Class, DataRepository>) repositoryMapField.get(null);
    }

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

            this.newObjects = (Queue<DomainObject>) newObjectsField.get(unit);
            this.clonedObjects = (Queue<DomainObject>) clonedObjectsField.get(unit);
            this.dirtyObjects = (Queue<DomainObject>) dirtyObjectsField.get(unit);
            this.removedObjects = (Queue<DomainObject>) removedObjectsField.get(unit);
        }
    }

    @Before
    public void before()throws NoSuchFieldException, IllegalAccessException {
        UnitOfWork.connectionsMap.values().forEach(array -> System.out.println(Arrays.toString(array)));
        assertEquals(0, UnitOfWork.numberOfOpenConnections.get());
        repositoryMap.clear();

        ConnectionManager manager = ConnectionManager.getConnectionManager(
                "jdbc:hsqldb:file:"+URLDecoder.decode(this.getClass().getClassLoader().getResource("testdb").getPath())+"/testdb",
                "SA", "");

        unit = new UnitOfWork(manager::getConnection);

        SQLConnection con = unit.getConnection().join();
        SqlUtils.<ResultSet>callbackToPromise(ar -> con.call("{call deleteDB()}", ar)).join();
        SqlUtils.<ResultSet>callbackToPromise(ar -> con.call("{call populateDB()}", ar)).join();
        //SQLUtils.<Void>callbackToPromise(con::commitNext).join();

        objectsContainer = new ObjectsContainer(con);
        unit.commit().join();
        setupLists();

        employeeRepo = MapperRegistry.getRepository(Employee.class);
        companyRepo = MapperRegistry.getRepository(Company.class);
    }

    @After
    public void after() {
        assertEquals(0, UnitOfWork.numberOfOpenConnections.get());
    }

    @Test
    public void testCommit() throws NoSuchFieldException, IllegalAccessException {
        populateIdentityMaps();
        addNewObjects();
        addDirtyAndClonedObjects();
        addRemovedObjects();

        List<DomainObject> newObjects = new ArrayList<>(this.newObjects);
        List<DomainObject> dirtyObjects = new ArrayList<>(this.dirtyObjects);
        List<DomainObject> removedObjects = new ArrayList<>(this.removedObjects);

        unit.commit().join();

        assertNewObjects(true);
        assertDirtyObjects(true);
        assertRemovedObjects(true);

        assertIdentityMaps(unit, newObjects, (identityMap, domainObject) -> assertEquals(((CompletableFuture<DomainObject>) identityMap.get(domainObject.getIdentityKey())).join(), domainObject));
        assertIdentityMaps(unit, dirtyObjects, (identityMap, domainObject) -> assertEquals(((CompletableFuture<DomainObject>) identityMap.get(domainObject.getIdentityKey())).join(), domainObject));
        assertIdentityMaps(unit, removedObjects, (identityMap, domainObject) -> assertNull(identityMap.get(domainObject.getIdentityKey())));
    }

    @Test
    public void testRollback() throws IllegalAccessException, NoSuchFieldException, SQLException {
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

        final boolean[] failed = {false};
        unit.commit()
                .exceptionally(throwable -> {
                    failed[0] = true;
                    return null;
                })
                .join();

        assertTrue(failed[0]);

        removedObjects.remove(chat);

        assertNewObjects(false);
        assertDirtyObjects(false);
        assertRemovedObjects(false);

        assertIdentityMaps(unit, newObjects, (identityMap, domainObject) -> assertNull(identityMap.get(domainObject.getIdentityKey())));
        assertIdentityMaps(unit, dirtyObjects, (identityMap, domainObject) -> {
            CompletableFuture<DomainObject> completableFuture = (CompletableFuture<DomainObject>) identityMap.get(domainObject.getIdentityKey());
            if(completableFuture != null)
                assertNotEquals(completableFuture.join(), domainObject);
            else
                assertNull(identityMap.get(domainObject.getIdentityKey()));
        });
        assertIdentityMaps(unit, removedObjects, (identityMap, domainObject) -> assertEquals(((CompletableFuture<DomainObject>) identityMap.get(domainObject.getIdentityKey())).join(), domainObject));
    }

    @Test
    public void testTransaction() {
        logger.info("Number of Openned connections - {}", UnitOfWork.numberOfOpenConnections.get());
        ConnectionManager connectionManager = ConnectionManager.getConnectionManager();
        SqlSupplier<CompletableFuture<SQLConnection>> connectionSqlSupplier = () -> connectionManager.getConnection(TransactionIsolation.READ_UNCOMMITTED.getType());

        Employee employee = employeeRepo.findWhere(unit, new Pair<>("name", "Bob")).join().get(0);
        Employee employee2 = employeeRepo.findWhere(unit, new Pair<>("name", "Charles")).join().get(0);
        unit.commit().join();

        UnitOfWork unitOfWork = new UnitOfWork(connectionSqlSupplier.wrap());
        CompletableFuture<Void> future1 = employeeRepo.deleteById(unitOfWork, employee.getIdentityKey());
        CompletableFuture<Void> future = employeeRepo.deleteById(unitOfWork, employee2.getIdentityKey());

        CompletableFuture.allOf(future, future1)
                .thenCompose(aVoid -> companyRepo.deleteById(unitOfWork, new Company.PrimaryKey(1, 1)))
                .thenCompose(voidCompletableFuture -> unitOfWork.commit())
                .join();

        assertTrue(employeeRepo.findWhere(unit, new Pair<>("name", "Bob")).join().isEmpty());
        assertTrue(employeeRepo.findWhere(unit, new Pair<>("name", "Charles")).join().isEmpty());
        assertTrue(!companyRepo.findById(unit, new Company.PrimaryKey(1, 1)).join().isPresent());
        unit.rollback().join();
    }

    private void assertIdentityMaps(UnitOfWork identityMapField, List<DomainObject> objectList, BiConsumer<ConcurrentMap, DomainObject> assertion) throws IllegalAccessException {
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
        Person person = null;
        Car car = null;
        TopStudent topStudent = null;
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
        DataRepository carRepo = getRepository(Car.class);
        Car originalCar = objectsContainer.getOriginalCar();
        unit.validate(originalCar.getIdentityKey(), originalCar);

        DataRepository employeeRepo = getRepository(Employee.class);
        Employee originalEmployee = objectsContainer.getOriginalEmployee();
        unit.validate(originalEmployee.getIdentityKey(), originalEmployee);
    }

    private void addRemovedObjects() {
        removedObjects.add(objectsContainer.getOriginalEmployee());
    }

    //Original car shall be in the identityMap
    private void addDirtyAndClonedObjects() {
        clonedObjects.add(objectsContainer.getOriginalPerson());
        clonedObjects.add(objectsContainer.getOriginalCar());
        clonedObjects.add(objectsContainer.getOriginalTopStudent());

        dirtyObjects.add(objectsContainer.getUpdatedPerson());
        dirtyObjects.add(objectsContainer.getUpdatedCar());
        dirtyObjects.add(objectsContainer.getUpdatedTopStudent());
    }

    private void addNewObjects() {
        newObjects.add(objectsContainer.getInsertedPerson());
        newObjects.add(objectsContainer.getInsertedCar());
        newObjects.add(objectsContainer.getInsertedTopStudent());
    }

    private <R extends DomainObject<P>, P> MapperRegistry.Container<R, P> getContainer(Class<R> rClass) {
        MapperSettings mapperSettings = new MapperSettings(rClass);
        ExternalsHandler<R, P> externalsHandler = new ExternalsHandler<>(mapperSettings);
        DataMapper<R, P> dataMapper = new DataMapper<>(rClass, externalsHandler, mapperSettings);
        Mapperify<R, P> mapperify = new Mapperify<>(dataMapper);
        Type type1 = ((ParameterizedType) rClass.getGenericInterfaces()[0]).getActualTypeArguments()[0];
        Comparator<R> comparator = new DomainObjectComparator<>(mapperSettings);
        DataRepository<R, P> employeeRepo = new DataRepository<>(rClass, mapperify, comparator);

        return new MapperRegistry.Container<>(mapperSettings, externalsHandler, employeeRepo, mapperify);
    }
}