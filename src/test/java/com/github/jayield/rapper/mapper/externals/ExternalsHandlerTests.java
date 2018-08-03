package com.github.jayield.rapper.mapper.externals;

import com.github.jayield.rapper.connections.ConnectionManager;
import com.github.jayield.rapper.utils.SqlUtils;
import com.github.jayield.rapper.unitofwork.UnitOfWork;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import org.junit.After;
import org.junit.Before;

import java.net.URLDecoder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExternalsHandlerTests {

    @Before
    public void before() {
        assertEquals(0, UnitOfWork.getNumberOfOpenConnections().get());
        ConnectionManager manager = ConnectionManager.getConnectionManager(
                "jdbc:hsqldb:file:" + URLDecoder.decode(this.getClass().getClassLoader().getResource("testdb").getPath()) + "/testdb",
                "SA",
                ""
        );

        UnitOfWork unit = new UnitOfWork(manager::getConnection);
        SQLConnection con = unit.getConnection().join();
        SqlUtils.<ResultSet>callbackToPromise(ar -> con.call("{call deleteDB()}", ar)).join();
        SqlUtils.<ResultSet>callbackToPromise(ar -> con.call("{call populateDB()}", ar)).join();
        unit.commit().join();
    }

    @After
    public void after() {
        assertEquals(0, UnitOfWork.getNumberOfOpenConnections().get());
    }

    /*@Test
    public void testInsertReferences() {
        Supplier<DataMapperException> object_not_found = () -> new DataMapperException("Object not found");
        UnitOfWork unit = new UnitOfWork();

        testInsertN1Relation(object_not_found, unit);
        testInsertNNRelation(object_not_found, unit);
    }

    @Test
    public void testUpdateReferences() {
        Supplier<DataMapperException> object_not_found = () -> new DataMapperException("Object not found");
        UnitOfWork unit = new UnitOfWork();

        testUpdateN1Relation(object_not_found, unit);
        testUpdateNNRelation(object_not_found, unit);
    }

    @Test
    public void testRemoveReferences() {
        Supplier<DataMapperException> object_not_found = () -> new DataMapperException("Object not found");
        UnitOfWork unit = new UnitOfWork();

        testRemoveN1Relation(object_not_found, unit);
        testRemoveNNRelation(object_not_found, unit);
    }

    private void testRemoveNNRelation(Supplier<DataMapperException> object_not_found, UnitOfWork unit) {
        //-------------------N-N Relation-------------------//
        //Arrange
        DataRepository<Author, Long> authorRepo = MapperRegistry.getRepository(Author.class);
        List<Author> authors = authorRepo.findWhere(unit, new Pair<>("name", "Ze")).join();
        assertEquals(1, authors.size());
        Author author = authors.get(0);

        //Act
        DataRepository<Book, Long> bookRepo = MapperRegistry.getRepository(Book.class);
        ExternalsHandler<Book, Long> bookEH = MapperRegistry.getExternal(Book.class);

        List<Book> books = bookRepo.findWhere(unit, new Pair<>("name", "1001 noites")).join();
        assertEquals(1, books.size());

        Book book = books.get(0);
        bookEH.removeReferences(book).join();

        //Assert
        Author author1 = authorRepo.findById(unit, author.getIdentityKey())
                .thenCompose(author2 -> unit.commit().thenApply(aVoid -> author2))
                .join()
                .orElseThrow(object_not_found);
        assertFalse(author1.getBooks().apply(unit).join().stream().anyMatch(book1 -> book1.getName().equals(book.getName())));
    }

    private void testRemoveN1Relation(Supplier<DataMapperException> object_not_found, UnitOfWork unit) {
        //-------------------N-1 Relation-------------------//
        //Arrange

        DataRepository<Company, Company.PrimaryKey> companyRepo = MapperRegistry.getRepository(Company.class);
        companyRepo.findById(unit, new Company.PrimaryKey(1, 1)).join().orElseThrow(object_not_found);

        //Act
        ExternalsHandler<Employee, Integer> employeeEH = MapperRegistry.getExternal(Employee.class);

        DataRepository<Employee, Integer> employeeRepo = MapperRegistry.getRepository(Employee.class);
        List<Employee> employees = employeeRepo.findWhere(unit, new Pair<>("name", "Bob")).join();
        assertEquals(1, employees.size());

        Employee employee = employees.get(0);
        employeeEH.removeReferences(employee).join();

        //Assert
        Company company = companyRepo.findById(unit, new Company.PrimaryKey(1, 1))
                .thenCompose(company1 -> unit.commit().thenApply(aVoid -> company1))
                .join()
                .orElseThrow(object_not_found);
        assertFalse(company.getEmployees().apply(unit).join().stream().anyMatch(employee1 -> employee1.getName().equals(employee.getName())));
    }

    private void testInsertNNRelation(Supplier<DataMapperException> object_not_found, UnitOfWork unit) {
        //-------------------N-N Relation-------------------//
        //Arrange
        DataRepository<Author, Long> authorRepo = MapperRegistry.getRepository(Author.class);
        List<Author> authors = authorRepo.findWhere(unit, new Pair<>("name", "Ze")).join();
        assertEquals(1, authors.size());
        Author author = authors.get(0);

        //Act
        ExternalsHandler<Book, Long> bookEH = MapperRegistry.getExternal(Book.class);
        CompletableFuture<List<Author>> future1 = authorRepo.findById(unit, author.getIdentityKey())
                .thenApply(author1 -> author1.orElseThrow(object_not_found))
                .thenApply(Arrays::asList);
        Book book = new Book(1, "Another test", 1, uW -> future1);
        bookEH.insertReferences(book).join();

        //Assert
        Author author1 = authorRepo.findById(unit, author.getIdentityKey())
                .thenCompose(author2 -> unit.commit().thenApply(aVoid -> author2))
                .join()
                .orElseThrow(object_not_found);
        assertTrue(future1.join().get(0).getBooks().apply(unit).join().stream().anyMatch(book1 -> book1.getName().equals("Another test")));
        assertTrue(author1.getBooks().apply(unit).join().stream().anyMatch(book1 -> book1.getName().equals("Another test")));
    }

    private void testInsertN1Relation(Supplier<DataMapperException> object_not_found, UnitOfWork unit) {
        //-------------------N-1 Relation-------------------//
        //Arrange

        DataRepository<Company, Company.PrimaryKey> companyRepo = MapperRegistry.getRepository(Company.class);
        companyRepo.findById(unit, new Company.PrimaryKey(1, 1)).join().orElseThrow(object_not_found);

        //Act
        ExternalsHandler<Employee, Integer> employeeEH = MapperRegistry.getExternal(Employee.class);
        CompletableFuture<Company> future = companyRepo.findById(unit, new Company.PrimaryKey(1, 1))
                .thenApply(company1 -> company1.orElseThrow(object_not_found));
        Employee employee = new Employee(1, "Teste", 1, new Foreign<>(new Company.PrimaryKey(1, 1), uW -> future));
        employeeEH.insertReferences(employee).join();

        //Assert
        Company company = companyRepo.findById(unit, new Company.PrimaryKey(1, 1))
                .thenCompose(company1 -> unit.commit().thenApply(aVoid -> company1))
                .join()
                .orElseThrow(object_not_found);
        assertTrue(future.join().getEmployees().apply(unit).join().stream().anyMatch(employee1 -> employee1.getName().equals("Teste")));
        assertTrue(company.getEmployees().apply(unit).join().stream().anyMatch(employee1 -> employee1.getName().equals("Teste")));
    }

    private void testUpdateN1Relation(Supplier<DataMapperException> object_not_found, UnitOfWork unit) {
        //-------------------N-1 Relation-------------------//
        //Arrange
        DataRepository<Company, Company.PrimaryKey> companyRepo = MapperRegistry.getRepository(Company.class);
        companyRepo.findById(unit, new Company.PrimaryKey(1, 1)).join().orElseThrow(object_not_found);
        companyRepo.findById(unit, new Company.PrimaryKey(1, 2)).join().orElseThrow(object_not_found);

        //Act
        ExternalsHandler<Employee, Integer> employeeEH = MapperRegistry.getExternal(Employee.class);
        CompletableFuture<Company> otherCompanyFuture = companyRepo.findById(unit, new Company.PrimaryKey(1, 2))
                .thenApply(company1 -> company1.orElseThrow(object_not_found));

        DataRepository<Employee, Integer> employeeRepo = MapperRegistry.getRepository(Employee.class);
        List<Employee> employees = employeeRepo.findWhere(unit, new Pair<>("name", "Bob")).join();
        assertEquals(1, employees.size());

        Employee oldEmployee = employees.get(0);
        Employee newEmployee = new Employee(oldEmployee.getIdentityKey(), "BobV2", oldEmployee.getVersion() + 1, new Foreign<>(new Company.PrimaryKey(1, 2), uW -> otherCompanyFuture));
        employeeEH.updateReferences(oldEmployee, newEmployee).join();

        //Assert
        Company company = companyRepo.findById(unit, new Company.PrimaryKey(1, 1))
                .thenCompose(company1 -> unit.commit().thenApply(aVoid -> company1))
                .join()
                .orElseThrow(object_not_found);
        assertTrue(otherCompanyFuture.join().getEmployees().apply(unit).join().stream().anyMatch(employee1 -> employee1.getName().equals("BobV2")));
        assertFalse(company.getEmployees().apply(unit).join().stream().anyMatch(employee1 -> employee1.getName().equals("Bob")));
    }

    private void testUpdateNNRelation(Supplier<DataMapperException> object_not_found, UnitOfWork unit) {
        //-------------------N-N Relation-------------------//
        //Arrange
        DataRepository<Author, Long> authorRepo = MapperRegistry.getRepository(Author.class);
        List<Author> authors = authorRepo.findWhere(unit, new Pair<>("name", "Ze")).join();
        assertEquals(1, authors.size());
        Author authorZe = authors.get(0);
        authors = authorRepo.findWhere(unit, new Pair<>("name", "Manel")).join();
        assertEquals(1, authors.size());
        Author authorManel = authors.get(0);

        //Act
        DataRepository<Book, Long> bookRepo = MapperRegistry.getRepository(Book.class);
        ExternalsHandler<Book, Long> bookEH = MapperRegistry.getExternal(Book.class);
        CompletableFuture<List<Author>> otherAuthorFuture = authorRepo.findById(unit, authorManel.getIdentityKey())
                .thenApply(author1 -> author1.orElseThrow(object_not_found))
                .thenApply(Arrays::asList);

        List<Book> books = bookRepo.findWhere(unit, new Pair<>("name", "1001 noites")).join();
        assertEquals(1, books.size());

        Book oldBook = books.get(0);
        Book newBook = new Book(oldBook.getIdentityKey(), "BookV2", oldBook.getVersion() + 1, uW -> otherAuthorFuture);
        bookEH.updateReferences(oldBook, newBook).join();

        //Assert
        Author author1 = authorRepo.findById(unit, authorZe.getIdentityKey())
                .thenCompose(author2 -> unit.commit().thenApply(aVoid -> author2))
                .join()
                .orElseThrow(object_not_found);
        assertTrue(otherAuthorFuture.join().get(0).getBooks().apply(unit).join().stream().anyMatch(book1 -> book1.getName().equals("BookV2")));
        assertFalse(author1.getBooks().apply(unit).join().stream().anyMatch(book1 -> book1.getName().equals("1001 noites")));
    }*/
}
