package com.github.jayield.rapper;

import com.github.jayield.rapper.domainModel.Account;
import com.github.jayield.rapper.utils.ConnectionManager;
import com.github.jayield.rapper.utils.DBsPath;
import com.github.jayield.rapper.utils.SqlSupplier;
import com.github.jayield.rapper.utils.UnitOfWork;
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
        Double join = CompletableFuture.supplyAsync(() -> Integer.parseInt("sasd")) // input String: "not detected"
                .thenApply(r -> r * 2 * Math.PI)
                //.thenApply(s -> "apply>> " + s)
                .exceptionally(ex -> Double.valueOf(0))
                /*.handle((result, ex) -> {
                    if (result != null) {
                        return result;
                    } else {
                        return ex;
                    }
                })*/
                .join();

        logger.info(join.toString());
    }

    @Test
    public void test2() throws SQLException {
        ConnectionManager manager = ConnectionManager.getConnectionManager(DBsPath.TESTDB);
        SqlSupplier<Connection> connectionSupplier = manager::getConnection;
        UnitOfWork.newCurrent(connectionSupplier.wrap());
        Connection connection = UnitOfWork.getCurrent().getConnection();

        logger.info(manager.getConnection().toString());

        PreparedStatement preparedStatement = connection.prepareStatement("delete from TopStudent where dbo.TopStudent.nif = 321");
        logger.info("" + preparedStatement.executeUpdate());

        connection.rollback();

        preparedStatement = connection.prepareStatement("select * from Person where nif = ?");
        preparedStatement.setInt(1, 2);
        ResultSet rs = preparedStatement.executeQuery();

        assertFalse(rs.next());

        logger.info(manager.getConnection().toString());
    }

    @Test
    public void test3(){
        DataMapper<Account, Long> accountMapper = new DataMapper<>(Account.class);

        logger.info("" + accountMapper.getInsertQuery());
    }
}
