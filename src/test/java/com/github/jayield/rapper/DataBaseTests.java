package com.github.jayield.rapper;

import com.github.jayield.rapper.connections.ConnectionManager;
import com.github.jayield.rapper.utils.SqlUtils;
import com.github.jayield.rapper.unitofwork.UnitOfWork;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URLDecoder;

import static org.junit.Assert.assertEquals;


public class DataBaseTests {

    private ConnectionManager connectionManager;

    @Before
    public void before() {
        connectionManager = ConnectionManager.getConnectionManager(
                "jdbc:hsqldb:file:" + URLDecoder.decode(this.getClass().getClassLoader().getResource("testdb").getPath()) + "/testdb",
                "SA",
                ""
        );

        assertEquals(0, UnitOfWork.numberOfOpenConnections.get());
    }

    @After
    public void after() {
        assertEquals(0, UnitOfWork.numberOfOpenConnections.get());
    }

    @Test
    public void connectivity() {
        try (SQLConnection con = connectionManager.getConnection().join()) {
            SqlUtils.<UpdateResult>callbackToPromise(ar ->
                    con.update("insert into Person(nif, name, birthday) values (1, 'Test', '1990-05-02')", ar))
                    .thenApply(UpdateResult::getUpdated)
                    .thenAccept(updated -> assertEquals(1, updated.intValue()))
                    .join();

            SqlUtils.<io.vertx.ext.sql.ResultSet>callbackToPromise(ar -> con.query("select * from Person", ar))
                    .thenApply(resultSet -> resultSet.getRows().isEmpty())
                    .thenAccept(Assert::assertFalse)
                    .join();

            SqlUtils.<UpdateResult>callbackToPromise(ar ->
                    con.updateWithParams("update Person set name = 'test2' where nif = ?", new JsonArray().add(1), ar))
                    .thenApply(UpdateResult::getUpdated)
                    .thenAccept(updated -> assertEquals(1, updated.intValue()))
                    .join();

            SqlUtils.<UpdateResult>callbackToPromise(ar ->
                    con.updateWithParams("delete from Person where nif = ?", new JsonArray().add(1), ar))
                    .thenApply(UpdateResult::getUpdated)
                    .thenAccept(updated -> assertEquals(1, updated.intValue()))
                    .join();
            SqlUtils.callbackToPromise(con::rollback)
                    .join();
        }
    }
}