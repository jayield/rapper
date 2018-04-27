package com.github.jayield.rapper;

import com.github.jayield.rapper.utils.ConnectionManager;
import com.github.jayield.rapper.utils.DBsPath;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataBaseTests {

    private ConnectionManager connectionManager;

    @Before
    public void before() throws SQLException {
        connectionManager = ConnectionManager.getConnectionManager(DBsPath.TESTDB);
        Connection con = connectionManager.getConnection();
        //DBStatements.createTables(con);
    }

    @Test
    public void crud() throws SQLException {
        try (Connection con = connectionManager.getConnection()) {
            PreparedStatement stmt = con.prepareStatement("insert into Person(nif, name, birthday) values (1, 'Test', '1990-05-02')");
            int update = stmt.executeUpdate();
            assertEquals(1, update);

            stmt = con.prepareStatement("select * from Person");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());

            stmt = con.prepareStatement("update Person set name = 'test2' where nif = ?");
            stmt.setInt(1, 1);
            update = stmt.executeUpdate();
            assertEquals(1, update);

            stmt = con.prepareStatement("delete from Person where nif = ?");
            stmt.setInt(1, 1);
            update = stmt.executeUpdate();
            assertEquals(1, update);

            con.rollback();
        }
    }
}