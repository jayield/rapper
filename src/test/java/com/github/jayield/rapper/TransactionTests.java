package com.github.jayield.rapper;

import com.github.jayield.rapper.domainModel.*;
import com.github.jayield.rapper.utils.*;
import org.junit.Before;
import org.junit.Test;

import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.SQLException;

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
    public void before() throws SQLException {
        ConnectionManager manager = ConnectionManager.getConnectionManager(
                "jdbc:hsqldb:file:"+URLDecoder.decode(this.getClass().getClassLoader().getResource("testdb").getPath())+"/testdb",
                "SA", "");
        SqlSupplier<Connection> connectionSupplier = manager::getConnection;
        UnitOfWork.newCurrent(connectionSupplier.wrap());
        Connection con = UnitOfWork.getCurrent().getConnection();

        con.prepareCall("{call deleteDB()}").execute();
        con.prepareCall("{call populateDB()}").execute();
        UnitOfWork.getCurrent().commit();
        UnitOfWork.removeCurrent();
    }

    @Test
    public void testSimpleTransaction() {
        Employee employee = employeeRepo.findWhere(new Pair<>("name", "Bob")).join().get(0);
        Employee employee2 = employeeRepo.findWhere(new Pair<>("name", "Charles")).join().get(0);

        new Transaction(Connection.TRANSACTION_READ_COMMITTED)
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
        new Transaction(Connection.TRANSACTION_READ_COMMITTED)
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
