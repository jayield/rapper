package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.*;
import com.github.jayield.rapper.domainModel.*;
import com.github.jayield.rapper.exceptions.DataMapperException;
import javafx.util.Pair;
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
import java.sql.Date;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

import static com.github.jayield.rapper.AssertUtils.*;
import static com.github.jayield.rapper.TestUtils.*;
import static com.github.jayield.rapper.utils.DBsPath.TESTDB;
import static com.github.jayield.rapper.utils.MapperRegistry.getRepository;
import static org.junit.Assert.*;

public class UnitOfWorkTests {

    private ObjectsContainer objectsContainer;
    private final Logger logger = LoggerFactory.getLogger(UnitOfWorkTests.class);
    private Queue<DomainObject> newObjects;
    private Queue<DomainObject> clonedObjects;
    private Queue<DomainObject> dirtyObjects;
    private Queue<DomainObject> removedObjects;
    private Connection con;
    private Map<Class, DataRepository> repositoryMap;

    public UnitOfWorkTests() throws NoSuchFieldException, IllegalAccessException {
        Field repositoryMapField = MapperRegistry.class.getDeclaredField("repositoryMap");
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

            this.newObjects = (Queue<DomainObject>) newObjectsField.get(UnitOfWork.getCurrent());
            this.clonedObjects = (Queue<DomainObject>) clonedObjectsField.get(UnitOfWork.getCurrent());
            this.dirtyObjects = (Queue<DomainObject>) dirtyObjectsField.get(UnitOfWork.getCurrent());
            this.removedObjects = (Queue<DomainObject>) removedObjectsField.get(UnitOfWork.getCurrent());
        }
    }

    @Before
    public void before() throws SQLException, NoSuchFieldException, IllegalAccessException {
        repositoryMap.clear();
        UnitOfWork.removeCurrent();
        ConnectionManager manager = ConnectionManager.getConnectionManager(
                "jdbc:hsqldb:file:"+URLDecoder.decode(this.getClass().getClassLoader().getResource("testdb").getPath())+"/testdb",
                "SA", "");
        SqlSupplier<Connection> connectionSupplier = manager::getConnection;
        UnitOfWork.newCurrent(connectionSupplier.wrap());
        con = UnitOfWork.getCurrent().getConnection();
        con.prepareCall("{call deleteDB()}").execute();
        con.prepareCall("{call populateDB()}").execute();
        con.commit();
        /*createTables(con);
        deleteDB(con);
        populateDB(con);*/

        objectsContainer = new ObjectsContainer(con);

        setupLists();
    }

    @After
    public void finish() throws SQLException {
        con.rollback();
        UnitOfWork.getCurrent().closeConnection();
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

        assertTrue(UnitOfWork.getCurrent().commit().join());
        //Get new connection since the commit will close the current one
        con = UnitOfWork.getCurrent().getConnection();

        Field identityMapField = DataRepository.class.getDeclaredField("identityMap");
        identityMapField.setAccessible(true);

        assertNewObjects(true);
        assertDirtyObjects(true);
        assertRemovedObjects(true);

        assertIdentityMaps(identityMapField, newObjects, (identityMap, domainObject) -> assertEquals(((CompletableFuture<DomainObject>) identityMap.get(domainObject.getIdentityKey())).join(), domainObject));
        assertIdentityMaps(identityMapField, dirtyObjects, (identityMap, domainObject) -> assertEquals(((CompletableFuture<DomainObject>) identityMap.get(domainObject.getIdentityKey())).join(), domainObject));
        assertIdentityMaps(identityMapField, removedObjects, (identityMap, domainObject) -> assertNull(identityMap.get(domainObject.getIdentityKey())));
    }

    @Test
    public void testUpdateReference() throws NoSuchFieldException, IllegalAccessException {
        DataRepository<Company, Company.PrimaryKey> companyRepo = MapperRegistry.getRepository(Company.class);

        MapperSettings mapperSettings = new MapperSettings(Employee.class);
        ExternalsHandler<Employee, Integer> externalHandler = new ExternalsHandler<>(mapperSettings);
        DataMapper<Employee, Integer> dataMapper = new DataMapper<>(Employee.class, mapperSettings, externalHandler);
        Mapperify<Employee, Integer> mapperify = new Mapperify<>(dataMapper);
        DataRepository<Employee, Integer> employeeRepo = new DataRepository<>(Employee.class, Integer.class, mapperify);

        MapperRegistry.Container<Employee, Integer> container = new MapperRegistry.Container<>(mapperSettings, externalHandler, employeeRepo, mapperify);

        //Place employeeRepo on MapperRegistry
        Field repositoryMapField = MapperRegistry.class.getDeclaredField("repositoryMap");
        repositoryMapField.setAccessible(true);
        HashMap<Class, MapperRegistry.Container> repositoryMap = (HashMap<Class, MapperRegistry.Container>) repositoryMapField.get(null);
        repositoryMap.put(Employee.class, container);

        //Create an Employee
        CompletableFuture<Company> companyCompletableFuture = companyRepo
                .findById(new Company.PrimaryKey(1, 1))
                .thenApply(company -> company.orElseThrow(() -> new DataMapperException(("Company not found"))));

        Employee employee = new Employee(0, "Hugo", 0, companyCompletableFuture);

        newObjects.add(employee);

        assertTrue(UnitOfWork.getCurrent().commit().join());
        //Get new connection since the commit will close the current one
        con = UnitOfWork.getCurrent().getConnection();

        //Check if employee is in the Company's list
        Company company = companyCompletableFuture.join();
        List<Employee> employees = company.getEmployees().join();
        assertTrue(employees.contains(employee));

        Field identityMapField = DataRepository.class.getDeclaredField("identityMap");
        identityMapField.setAccessible(true);

        ConcurrentMap employeeIdentityMap = (ConcurrentMap) identityMapField.get(employeeRepo);

        CompletableFuture<Employee> employeeCP = (CompletableFuture<Employee>) employeeIdentityMap.get(employee.getIdentityKey());

        assertEquals(employeeCP.join(), employee);
        assertEquals(1, mapperify.getIfindWhere().getCount());
    }

    @Test
    public void testMultipleReference() throws NoSuchFieldException, IllegalAccessException {
        MapperRegistry.Container<Author, Long> authorContainer = getContainer(Author.class);
        MapperRegistry.Container<Book, Long> bookContainer = getContainer(Book.class);

        //Place employeeRepo on MapperRegistry
        Field repositoryMapField = MapperRegistry.class.getDeclaredField("repositoryMap");
        repositoryMapField.setAccessible(true);

        HashMap<Class, MapperRegistry.Container> repositoryMap = (HashMap<Class, MapperRegistry.Container>) repositoryMapField.get(null);
        repositoryMap.put(Author.class, authorContainer);
        repositoryMap.put(Book.class, bookContainer);

        CompletableFuture<List<Author>> authorCP = authorContainer.getDataRepository().findWhere(new Pair<>("name", "Ze"));
        authorCP.join();
        Book book = new Book(0, "Harry Potter", 0, authorCP);

        newObjects.add(book);

        assertTrue(UnitOfWork.getCurrent().commit().join());
        //Get new connection since the commit will close the current one
        con = UnitOfWork.getCurrent().getConnection();

        //Check if book is in the author's list
        Author author = authorCP.join().get(0);
        List<Book> books = author.getBooks().join();
        assertTrue(books.contains(book));

        Field identityMapField = DataRepository.class.getDeclaredField("identityMap");
        identityMapField.setAccessible(true);

        ConcurrentMap bookIdentityMap = (ConcurrentMap) identityMapField.get(bookContainer.getDataRepository());

        CompletableFuture<Book> bookCP = (CompletableFuture<Book>) bookIdentityMap.get(book.getIdentityKey());

        assertEquals(bookCP.join(), book);
    }

    @Test
    public void testRollback() throws IllegalAccessException, NoSuchFieldException {
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
        //Get new connection since the commit will close the current one
        con = UnitOfWork.getCurrent().getConnection();

        removedObjects.remove(chat);
        Field identityMapField = DataRepository.class.getDeclaredField("identityMap");
        identityMapField.setAccessible(true);

        assertNewObjects(false);
        assertDirtyObjects(false);
        assertRemovedObjects(false);

        assertIdentityMaps(identityMapField, newObjects, (identityMap, domainObject) -> assertNull(identityMap.get(domainObject.getIdentityKey())));
        assertIdentityMaps(identityMapField, dirtyObjects, (identityMap, domainObject) -> {
            CompletableFuture<DomainObject> completableFuture = (CompletableFuture<DomainObject>) identityMap.get(domainObject.getIdentityKey());
            if(completableFuture != null)
                assertNotEquals(completableFuture.join(), domainObject);
            else
                assertNull(identityMap.get(domainObject.getIdentityKey()));
        });
        assertIdentityMaps(identityMapField, removedObjects, (identityMap, domainObject) -> assertEquals(((CompletableFuture<DomainObject>) identityMap.get(domainObject.getIdentityKey())).join(), domainObject));
    }

    private void assertIdentityMaps(Field identityMapField, List<DomainObject> objectList, BiConsumer<ConcurrentMap, DomainObject> assertion) throws IllegalAccessException {
        for (DomainObject domainObject : objectList) {
            DataRepository repository = getRepository(domainObject.getClass());
            ConcurrentMap identityMap = (ConcurrentMap) identityMapField.get(repository);
            assertion.accept(identityMap, domainObject);
        }
    }

    private void assertRemovedObjects(boolean isCommit) {
        Employee originalEmployee = objectsContainer.getOriginalEmployee();

        if(isCommit) {
            assertNotFound(employeeSelectQuery, getEmployeePSConsumer(originalEmployee.getName()), con);
        }
        else{
            assertSingleRow(originalEmployee, employeeSelectQuery, getEmployeePSConsumer(originalEmployee.getName()), AssertUtils::assertEmployee, con);
        }
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

        assertSingleRow(person, personSelectQuery, getPersonPSConsumer(person.getNif()), AssertUtils::assertPerson, con);
        assertSingleRow(car, carSelectQuery, getCarPSConsumer(car.getIdentityKey().getOwner(), car.getIdentityKey().getPlate()), AssertUtils::assertCar, con);
        assertSingleRow(topStudent, topStudentSelectQuery, getPersonPSConsumer(topStudent.getNif()), AssertUtils::assertTopStudent, con);
    }

    private void assertNewObjects(boolean isCommit) {
        Person person = objectsContainer.getInsertedPerson();
        Car car = objectsContainer.getInsertedCar();
        TopStudent topStudent = objectsContainer.getInsertedTopStudent();

        if(isCommit) {
            assertSingleRow(person, personSelectQuery, getPersonPSConsumer(person.getNif()), AssertUtils::assertPerson, con);
            assertSingleRow(car, carSelectQuery, getCarPSConsumer(car.getIdentityKey().getOwner(), car.getIdentityKey().getPlate()), AssertUtils::assertCar, con);
            assertSingleRow(topStudent, topStudentSelectQuery, getPersonPSConsumer(topStudent.getNif()), AssertUtils::assertTopStudent, con);
        }
        else {
            assertNotFound(personSelectQuery, getPersonPSConsumer(person.getNif()), con);
            assertNotFound(carSelectQuery, getCarPSConsumer(car.getIdentityKey().getOwner(), car.getIdentityKey().getPlate()), con);
            assertNotFound(topStudentSelectQuery, getPersonPSConsumer(topStudent.getNif()), con);
        }
    }

    private void populateIdentityMaps() {
        DataRepository carRepo = getRepository(Car.class);
        Car originalCar = objectsContainer.getOriginalCar();
        carRepo.validate(originalCar.getIdentityKey(), originalCar);

        DataRepository employeeRepo = getRepository(Employee.class);
        Employee originalEmployee = objectsContainer.getOriginalEmployee();
        employeeRepo.validate(originalEmployee.getIdentityKey(), originalEmployee);
    }

    private void addRemovedObjects() {
        removedObjects.add(objectsContainer.getOriginalEmployee());
    }

    //Original car shall be in the identityMap
    private void addDirtyAndClonedObjects() {
        clonedObjects.add(objectsContainer.getOriginalCar());

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
        ExternalsHandler<R, P> externalHandler = new ExternalsHandler<>(mapperSettings);
        DataMapper<R, P> dataMapper = new DataMapper<>(rClass, mapperSettings, externalHandler);
        Mapperify<R, P> mapperify = new Mapperify<>(dataMapper);
        Type type1 = ((ParameterizedType) rClass.getGenericInterfaces()[0]).getActualTypeArguments()[0];
        DataRepository<R, P> employeeRepo = new DataRepository<>(rClass, (Class<P>) type1, mapperify);

        return new MapperRegistry.Container<>(mapperSettings, externalHandler, employeeRepo, mapperify);
    }
}