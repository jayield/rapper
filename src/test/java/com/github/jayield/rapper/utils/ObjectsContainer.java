package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.domainModel.*;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

import static com.github.jayield.rapper.TestUtils.*;
import static com.github.jayield.rapper.TestUtils.getEmployeePSConsumer;

class ObjectsContainer {
    private final Person originalPerson;
    private final Person insertedPerson;
    private final Car insertedCar;
    private final TopStudent insertedTopStudent;
    private final Person updatedPerson;
    private final Car updatedCar;
    private final TopStudent updatedTopStudent;
    private final Car originalCar;
    private final Employee originalEmployee;
    private final TopStudent originalTopStudent;

    ObjectsContainer() throws SQLException {
        insertedPerson = new Person(123, "abc", new Date(1969, 6, 9), 0);
        insertedCar = new Car(1, "58en60", "Mercedes", "ES1", 0);
        insertedTopStudent = new TopStudent(456, "Manel", new Date(2020, 12, 1), 0, 1, 20, 2016, 0, 0);

        ResultSet rs = executeQuery("select CAST(version as bigint) version from Person where nif = ?", getPersonPSConsumer(321));
        updatedPerson = new Person(321, "Maria", new Date(2010, 2, 3), rs.getLong(1));

        rs = executeQuery("select CAST(version as bigint) version from Car where owner = ? and plate = ?", getCarPSConsumer(2, "23we45"));
        updatedCar = new Car(2, "23we45", "Mitsubishi", "lancer evolution", rs.getLong(1));

        rs = executeQuery("select CAST(P.version as bigint), CAST(S2.version as bigint), CAST(TS.version as bigint) version from Person P " +
                "inner join Student S2 on P.nif = S2.nif " +
                "inner join TopStudent TS on S2.nif = TS.nif where P.nif = ?", getPersonPSConsumer(454));
        updatedTopStudent = new TopStudent(454, "Carlos", new Date(2010, 6, 3), rs.getLong(2),
                4, 6, 7, rs.getLong(3), rs.getLong(1));

        rs = executeQuery(personSelectQuery, getPersonPSConsumer(321));
        originalPerson = new Person(rs.getInt("nif"), rs.getString("name"), rs.getDate("birthday"), rs.getLong("version"));

        rs = executeQuery(carSelectQuery, getCarPSConsumer(2, "23we45"));
        originalCar = new Car(rs.getInt("owner"), rs.getString("plate"), rs.getString("brand"), rs.getString("model"), rs.getLong("version"));

        rs = executeQuery(topStudentSelectQuery, getPersonPSConsumer(454));
        originalTopStudent = new TopStudent(rs.getInt("nif"), rs.getString("name"), rs.getDate("birthday"), rs.getLong("P1version"), rs.getInt("studentNumber"),
                rs.getInt("topGrade"), rs.getInt("year"), rs.getLong("Cversion"), rs.getLong("P2version"));

        rs = executeQuery(companySelectQuery, getCompanyPSConsumer(1, 1));
        Company originalCompany = new Company(new Company.PrimaryKey(rs.getInt("id"), rs.getInt("cid")), rs.getString("motto"), null, rs.getLong("Cversion"));

        rs = executeQuery(employeeSelectQuery, getEmployeePSConsumer("Charles"));
        originalEmployee = new Employee(rs.getInt("id"), rs.getString("name"), rs.getLong("version"), CompletableFuture.completedFuture(originalCompany));
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