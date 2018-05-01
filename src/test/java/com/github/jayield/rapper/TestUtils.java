package com.github.jayield.rapper;

import com.github.jayield.rapper.utils.DBsPath;
import com.github.jayield.rapper.utils.SqlConsumer;
import com.github.jayield.rapper.utils.UnitOfWork;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Consumer;

import static com.github.jayield.rapper.utils.ConnectionManager.getConnectionManager;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class TestUtils {
    public static final String personSelectQuery = "select nif, name, birthday, CAST(version as bigint) version from Person where nif = ?";
    public static final String bookSelectQuery = "select id, name, CAST(version as bigint) version from Book where name = ?";
    public static final String carSelectQuery = "select owner, plate, brand, model, CAST(version as bigint) version from Car where owner = ? and plate = ?";
    public static final String employeeSelectQuery = "select id, name, companyId, companyCid, CAST(version as bigint) version from Employee where name = ?";
    public static final String studentSelectQuery = "select C.studentNumber, CAST(C.version as bigint) Cversion, P1.name, P1.birthday, CAST(P1.version as bigint) P1version, P1.nif\n" +
            "  from Student C inner join Person P1 on C.nif = P1.nif where C.nif = ?";
    public static final String topStudentSelectQuery = "select P1.studentNumber, CAST(P1.version as bigint) P1version, P2.name, P2.birthday, CAST(P2.version as bigint) P2version, P2.nif, C.topGrade, C.year,\n" +
            "  CAST(C.version as bigint) Cversion from TopStudent C inner join Student P1 on C.nif = P1.nif inner join Person P2 on P1.nif = P2.nif where C.nif = ?";
    public static final String companySelectQuery = "select C.id, C.cid, C.motto, CAST(C.version as bigint) Cversion from [Company] C where id = ? and cid = ?";

    public static ResultSet executeQuery(String sql, Consumer<PreparedStatement> preparedStatementConsumer){
        try {
            PreparedStatement ps = getConnectionManager(DBsPath.TESTDB).getConnection().prepareStatement(sql);
            preparedStatementConsumer.accept(ps);
            ResultSet rs = ps.executeQuery();
            rs.next();
            return rs;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Consumer<PreparedStatement> getPersonPSConsumer(int nif) {
        SqlConsumer<PreparedStatement> consumer = ps -> ps.setInt(1, nif);
        return consumer.wrap();
    }

    public static Consumer<PreparedStatement> getCompanyPSConsumer(int companyId, int companyCid) {
        SqlConsumer<PreparedStatement> companyPSConsumer = ps ->{
            ps.setInt(1, companyId);
            ps.setInt(2, companyCid);
        };
        return companyPSConsumer.wrap();
    }

    public static Consumer<PreparedStatement> getCarPSConsumer(int owner, String plate) {
        SqlConsumer<PreparedStatement> carPSConsumer = ps ->{
            ps.setInt(1, owner);
            ps.setString(2, plate);
        };
        return carPSConsumer.wrap();
    }

    public static Consumer<PreparedStatement> getEmployeePSConsumer(String name){
        SqlConsumer<PreparedStatement> consumer = ps -> {
            ps.setString(1, name);
        };

        return consumer.wrap();
    }

    public static Consumer<PreparedStatement> getBookPSConsumer(String name){
        SqlConsumer<PreparedStatement> consumer = ps -> {
            ps.setString(1, name);
        };

        return consumer.wrap();
    }
}
