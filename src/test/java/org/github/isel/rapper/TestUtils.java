package org.github.isel.rapper;

import org.github.isel.rapper.utils.SqlConsumer;
import org.github.isel.rapper.utils.UnitOfWork;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class TestUtils {
    public static final String personSelectQuery = "select nif, name, birthday, CAST(version as bigint) version from Person where nif = ?";
    public static final String carSelectQuery = "select owner, plate, brand, model, CAST(version as bigint) version from Car where owner = ? and plate = ?";
    public static final String employeeSelectQuery = "select id, name, companyId, companyCid, CAST(version as bigint) version from Employee where name = ?";
    public static final String topStudentSelectQuery = "select P.nif, P.name, P.birthday, S2.studentNumber, TS.topGrade, TS.year, CAST(TS.version as bigint) version from Person P " +
            "inner join Student S2 on P.nif = S2.nif " +
            "inner join TopStudent TS on S2.nif = TS.nif where P.nif = ?";

    public static ResultSet executeQuery(String sql, Consumer<PreparedStatement> preparedStatementConsumer){
        try {
            PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement(sql);
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

    public static Consumer<PreparedStatement> getTopStudentPSConsumer(int nif) {
        SqlConsumer<PreparedStatement> consumer = ps -> {
            ps.setInt(1, nif);
        };
        return consumer.wrap();
    }

    public static Consumer<PreparedStatement> getEmployeePSConsumer(String name){
        SqlConsumer<PreparedStatement> consumer = ps -> {
            ps.setString(1, name);
        };

        return consumer.wrap();
    }

    public static<U> void assertSingleRow(UnitOfWork current, U object, String sql, Consumer<PreparedStatement> prepareStatement, BiConsumer<U, ResultSet> assertConsumer) {
        try{
            PreparedStatement ps = current.getConnection().prepareStatement(sql);
            prepareStatement.accept(ps);
            ResultSet rs = ps.executeQuery();

            if(rs.next())
                assertConsumer.accept(object, rs);
            else fail("Object wasn't selected from the database");
        }
        catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    public static<U> void assertMultipleRows(UnitOfWork current, List<U> list, String sql, BiConsumer<U, ResultSet> assertConsumer, int expectedRows){
        try {
            PreparedStatement ps = current.getConnection().prepareStatement(sql,
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = ps.executeQuery();

            rs.last();
            assertEquals(expectedRows, rs.getRow());
            rs.beforeFirst();

            if (rs.next())
                assertConsumer.accept(list.get(0), rs);
            else fail("Objects weren't selected from the database");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertDelete(String sql, Consumer<PreparedStatement> prepareStatement){
        try {
            PreparedStatement ps = UnitOfWork.getCurrent().getConnection().prepareStatement(sql);
            prepareStatement.accept(ps);
            ResultSet rs = ps.executeQuery();
            assertFalse(rs.next());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
