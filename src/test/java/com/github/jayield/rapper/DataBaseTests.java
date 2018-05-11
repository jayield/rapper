package com.github.jayield.rapper;

import com.github.jayield.rapper.domainModel.Company;
import com.github.jayield.rapper.domainModel.Employee;
import com.github.jayield.rapper.utils.ConnectionManager;
import com.github.jayield.rapper.utils.DBsPath;
import com.github.jayield.rapper.utils.MapperRegistry;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.ParameterizedType;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.github.jayield.rapper.TestUtils.bookSelectQuery;
import static com.github.jayield.rapper.TestUtils.executeQuery;
import static com.github.jayield.rapper.TestUtils.getBookPSConsumer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataBaseTests {

    private ConnectionManager connectionManager;
    private Connection con;

    @Before
    public void before() throws SQLException {
        connectionManager = ConnectionManager.getConnectionManager(
                "jdbc:hsqldb:file:"+URLDecoder.decode(this.getClass().getClassLoader().getResource("testdb").getPath())+"/testdb",
                "SA", "");
        con = connectionManager.getConnection();
    }

    @Test
    public void connectivity() throws SQLException {
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

    /*@Test
    public void test() throws SQLException {
        ResultSet rs = executeQuery(bookSelectQuery, getBookPSConsumer("1001 noites"), con);

        Object o = rs.getObject("id", Long.class);
    }*/
}