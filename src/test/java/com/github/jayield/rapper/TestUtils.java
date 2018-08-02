package com.github.jayield.rapper;

import com.github.jayield.rapper.utils.SqlUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;

import static com.github.jayield.rapper.connections.ConnectionManager.getConnectionManager;
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
    public static final String companySelectQuery = "select C.id, C.cid, C.motto, CAST(C.version as bigint) Cversion from Company C where id = ? and cid = ?";
    public static final String companySelectTop10Query = "select C.id, C.cid, C.motto, CAST(C.version as bigint) Cversion from Company C  order by id, cid offset 0 rows fetch next 10 rows only";
    public static final String dogSelectQuery = "select name, race, age from Dog where name = ? and race = ?";

    public static ResultSet executeQuery(String sql, JsonArray jsonArray, SQLConnection con){
         return SqlUtils.<io.vertx.ext.sql.ResultSet>callbackToPromise(ar -> con.queryWithParams(sql, jsonArray, ar)).join();
    }
}
