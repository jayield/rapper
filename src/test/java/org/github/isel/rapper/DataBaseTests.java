package org.github.isel.rapper;

import org.github.isel.rapper.utils.ConnectionManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class DataBaseTests {

    private static final String envVarName = "DBTEST_CONNECTION_STRING";
    private static final ConnectionManager manager = new ConnectionManager(envVarName);

    public static Connection getConnection() {
        return manager.getConnection();
    }

    private Connection con;

    @Before
    public void start() throws SQLException {
        con = getConnection();
        con.setAutoCommit(false);
    }

    @After
    public void finish() throws SQLException {
        con.rollback();
        con.close();
    }

    @Test
    public void test() throws SQLException {
        PreparedStatement s = con.prepareStatement("select * from Person");
        ResultSet rs = s.executeQuery();
        rs.next();
        for (int i = 0; i < 10000; i++) {
            System.out.println(rs.getObject("nif"));
        }

        System.out.println(rs.getObject("nif"));
    }

    @Test
    public void CRUDTest() throws SQLException {
        PreparedStatement statement = con.prepareStatement("INSERT INTO ApiDatabase.[Local] ([Address], Country) Values (?, ?)");
        statement.setString(1, "Rua do Teste da Base de Dados");
        statement.setString(2, "Dataland");
        statement.executeUpdate();

        statement = con.prepareStatement("SELECT * FROM ApiDatabase.[Local] WHERE Country = ?");
        statement.setString(1, "Dataland");
        ResultSet rs = statement.executeQuery();

        assertTrue(rs.next());
        assertEquals("Rua do Teste da Base de Dados", rs.getString("Address"));

        statement = con.prepareStatement("UPDATE ApiDatabase.[Local] SET ZIPCode = 404 WHERE [Address] = ?");
        statement.setString(1, "Rua do Teste da Base de Dados");
        int rows = statement.executeUpdate();

        assertEquals(1, rows);

        statement = con.prepareStatement("DELETE FROM ApiDatabase.[Local] WHERE [Address] = ?");
        statement.setString(1, "Rua do Teste da Base de Dados");
        rows = statement.executeUpdate();

        assertEquals(1, rows);
    }
}
