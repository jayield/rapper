package org.github.isel.rapper;

import org.github.isel.rapper.utils.ConnectionManager;
import org.github.isel.rapper.utils.DBsPath;
import org.github.isel.rapper.utils.UnitOfWork;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertFalse;

public class Tests {

    private Logger logger = LoggerFactory.getLogger(Tests.class);
    private AtomicInteger integer = new AtomicInteger(0);

    @Test
    public void test1(){
        CompletableFuture.supplyAsync(() -> Integer.parseInt("1")) // input String: "not detected"
                .thenApply(r -> r * 2 * Math.PI)
                //.thenApply(s -> "apply>> " + s)
                //.exceptionally(ex -> "Error: " + ex.getMessage())
                .handle((result, ex) -> {
                    if (result != null) {
                        return result;
                    } else {
                        return ex;
                    }
                })
                .join();

        System.out.println("");
    }

    @Test
    public void test2() throws SQLException {
        ConnectionManager manager = ConnectionManager.getConnectionManager(DBsPath.TESTDB);
        UnitOfWork.newCurrent(manager::getConnection);
        Connection connection = UnitOfWork.getCurrent().getConnection();

        System.out.println(connection);

        PreparedStatement preparedStatement = connection.prepareStatement("INSERT into Person(nif) values (2)");
        preparedStatement.executeUpdate();

        connection.rollback();

        preparedStatement = connection.prepareStatement("select * from Person where nif = ?");
        preparedStatement.setInt(1, 2);
        ResultSet rs = preparedStatement.executeQuery();

        assertFalse(rs.next());

        System.out.println(manager.getConnection());
    }
}
