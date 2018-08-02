package com.github.jayield.rapper.unitofwork;

import com.github.jayield.rapper.domainModel.*;
import com.github.jayield.rapper.mapper.externals.Foreign;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

import static com.github.jayield.rapper.TestUtils.*;

class ObjectsContainer {
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private Person originalPerson;
    private Person insertedPerson;
    private Car insertedCar;
    private TopStudent insertedTopStudent;
    private Person updatedPerson;
    private Car updatedCar;
    private TopStudent updatedTopStudent;
    private Car originalCar;
    private Employee originalEmployee;
    private TopStudent originalTopStudent;

    ObjectsContainer(SQLConnection con) {
        try {
            insertedPerson = new Person(123, "abc", new Date(1969, 6, 9).toInstant(), 0);
            insertedCar = new Car(1, "58en60", "Mercedes", "ES1", 0);
            insertedTopStudent = new TopStudent(456, "Manel", new Date(2020, 12, 1).toInstant(), 0, 1, 20, 2016, 0, 0);

            ResultSet rs = executeQuery("select CAST(version as bigint) version from Person where nif = ?", new JsonArray().add(321), con);
            updatedPerson = new Person(321, "Maria", new Date(2010, 2, 3).toInstant(), rs.getResults().get(0).getLong(0));

            rs = executeQuery("select CAST(version as bigint) version from Car where owner = ? and plate = ?", new JsonArray().add(2).add("23we45"), con);
            updatedCar = new Car(2, "23we45", "Mitsubishi", "lancer evolution", rs.getResults().get(0).getLong(0));

            rs = executeQuery("select CAST(P.version as bigint), CAST(S2.version as bigint), CAST(TS.version as bigint) version from Person P " +
                    "inner join Student S2 on P.nif = S2.nif " +
                    "inner join TopStudent TS on S2.nif = TS.nif where P.nif = ?", new JsonArray().add(454), con);
            updatedTopStudent = new TopStudent(454, "Carlos", new Date(2010, 6, 3).toInstant(), rs.getResults().get(0).getLong(1),
                    4, 6, 7, rs.getResults().get(0).getLong(2), rs.getResults().get(0).getLong(0));

            rs = executeQuery(personSelectQuery, new JsonArray().add(321), con);
            originalPerson = new Person(rs.getRows(true).get(0).getInteger("nif"), rs.getRows(true).get(0).getString("name"), sdf.parse(rs.getRows(true).get(0).getString("birthday")).toInstant(), rs.getRows(true).get(0).getLong("version"));

            rs = executeQuery(carSelectQuery, new JsonArray().add(2).add("23we45"), con);
            JsonObject first = rs.getRows(true).get(0);
            originalCar = new Car(first.getInteger("owner"), first.getString("plate"), first.getString("brand"), first.getString("model"), first.getLong("version"));

            rs = executeQuery(topStudentSelectQuery, new JsonArray().add(454), con);
            first = rs.getRows(true).get(0);
            originalTopStudent = new TopStudent(first.getInteger("nif"), first.getString("name"), sdf.parse(first.getString("birthday")).toInstant(), first.getLong("P1version"), first.getInteger("studentNumber"),
                    first.getInteger("topGrade"), first.getInteger("year"), first.getLong("Cversion"), first.getLong("P2version"));

            rs = executeQuery(companySelectQuery, new JsonArray().add(1).add(1), con);
            first = rs.getRows(true).get(0);
            Company originalCompany = new Company(new Company.PrimaryKey(first.getInteger("id"), first.getInteger("cid")), first.getString("motto"), null, first.getLong("Cversion"));

            rs = executeQuery(employeeSelectQuery, new JsonArray().add("Charles"), con);
            first = rs.getRows(true).get(0);
            originalEmployee = new Employee(first.getInteger("id"), first.getString("name"), first.getLong("version"), new Foreign<>(originalCompany.getIdentityKey(), unit -> CompletableFuture.completedFuture(originalCompany)));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public TopStudent getOriginalTopStudent() {
        return originalTopStudent;
    }

    public Person getOriginalPerson() {
        return originalPerson;
    }

    public Person getInsertedPerson() {
        return insertedPerson;
    }

    public Car getInsertedCar() {
        return insertedCar;
    }

    public TopStudent getInsertedTopStudent() {
        return insertedTopStudent;
    }

    public Person getUpdatedPerson() {
        return updatedPerson;
    }

    public Car getUpdatedCar() {
        return updatedCar;
    }

    public TopStudent getUpdatedTopStudent() {
        return updatedTopStudent;
    }

    public Car getOriginalCar() {
        return originalCar;
    }

    public Employee getOriginalEmployee() {
        return originalEmployee;
    }
}