package com.github.jayield.rapper;

import com.github.jayield.rapper.domainModel.*;
import com.github.jayield.rapper.utils.*;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.TransactionIsolation;
import org.junit.Before;
import org.junit.Test;

import java.net.URLDecoder;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;

public class TransactionTests {

    private final DataRepository<TopStudent, Integer> topStudentRepo;
    private final DataRepository<Person, Integer> personRepo;
    private final DataRepository<Employee, Integer> employeeRepo;
    private final DataRepository<Company, Company.PrimaryKey> companyRepo;
    private final DataRepository<Book, Long> bookRepo;
    private final DataRepository<Author, Long> authorRepo;

    public TransactionTests() {
        topStudentRepo = MapperRegistry.getRepository(TopStudent.class);
        personRepo = MapperRegistry.getRepository(Person.class);
        employeeRepo = MapperRegistry.getRepository(Employee.class);
        companyRepo = MapperRegistry.getRepository(Company.class);
        bookRepo = MapperRegistry.getRepository(Book.class);
        authorRepo = MapperRegistry.getRepository(Author.class);
    }

    @Before
    public void before() {
        ConnectionManager manager = ConnectionManager.getConnectionManager(
                "jdbc:hsqldb:file:"+URLDecoder.decode(this.getClass().getClassLoader().getResource("testdb").getPath())+"/testdb",
                "SA", "");
        SqlSupplier<CompletableFuture<SQLConnection>> connectionSupplier = manager::getConnection;
        UnitOfWork.newCurrent(connectionSupplier.wrap());
        CompletableFuture<SQLConnection> con = UnitOfWork.getCurrent().getConnection();

        con.thenCompose(sqlConnection ->
                SQLUtils.<ResultSet>callbackToPromise(ar -> sqlConnection.call("{call deleteDB()}", ar))
                    .thenAccept(v -> SQLUtils.<ResultSet>callbackToPromise(ar -> sqlConnection.call("{call populateDB()}", ar)))
        ).join();
        UnitOfWork.getCurrent().commit().join();
        UnitOfWork.removeCurrent();
    }

    @Test
    public void testSimpleTransaction() {
        Employee employee = employeeRepo.findWhere(new Pair<>("name", "Bob")).join().get(0);
        Employee employee2 = employeeRepo.findWhere(new Pair<>("name", "Charles")).join().get(0);

        new Transaction(TransactionIsolation.READ_COMMITTED.getType())
                .andDo(() -> employeeRepo.deleteById(employee.getIdentityKey()))
                .andDo(() -> employeeRepo.deleteById(employee2.getIdentityKey()))
                .andDo(() -> companyRepo.deleteById(new Company.PrimaryKey(1, 1)))
                .commit()
                .exceptionally(throwable -> {
                    fail();
                    return null;
                })
                .join();

        assertTrue(employeeRepo.findWhere(new Pair<>("name", "Bob")).join().isEmpty());
        assertTrue(employeeRepo.findWhere(new Pair<>("name", "Charles")).join().isEmpty());
        assertTrue(!companyRepo.findById(new Company.PrimaryKey(1, 1)).join().isPresent());
    }

    @Test
    public void testTransactionRollback() {
        Employee employee = employeeRepo.findWhere(new Pair<>("name", "Bob")).join().get(0);

        final boolean[] failed = {false};
        new Transaction(TransactionIsolation.READ_COMMITTED.getType())
                .andDo(() -> employeeRepo.deleteById(employee.getIdentityKey()))
                .andDo(() -> companyRepo.deleteById(new Company.PrimaryKey(1, 1)))
                .commit()
                .exceptionally(throwable -> {
                    failed[0] = true;
                    return null;
                })
                .join();

        assertTrue(failed[0]);

        assertFalse(employeeRepo.findWhere(new Pair<>("name", "Bob")).join().isEmpty());
        assertTrue(companyRepo.findById(new Company.PrimaryKey(1, 1)).join().isPresent());
    }
}
