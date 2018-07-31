package com.github.jayield.rapper;

import com.github.jayield.rapper.domainModel.*;
import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.utils.SqlUtils;
import com.github.jayield.rapper.utils.UnitOfWork;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;

import java.lang.reflect.Field;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static org.junit.Assert.*;

public class AssertUtils {
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    //---------------------Domain Objects assertions----------------------------------
    public static void assertPerson(Person person, JsonObject rs) {
        try {
            assertEquals(person.getNif(), rs.getInteger("nif").intValue());
            assertEquals(person.getName(), rs.getString("name"));
            assertEquals(person.getBirthday(), sdf.parse(rs.getString("birthday")).toInstant());
            assertEquals(person.getVersion(), rs.getLong("version").longValue());
            assertNotEquals(0, person.getVersion());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertCar(Car car, JsonObject rs) {
        assertEquals(car.getIdentityKey().getOwner(), rs.getInteger("owner").intValue());
        assertEquals(car.getIdentityKey().getPlate(), rs.getString("plate"));
        assertEquals(car.getBrand(), rs.getString("brand"));
        assertEquals(car.getModel(), rs.getString("model"));
        assertEquals(car.getVersion(), rs.getLong("version").longValue());
        assertNotEquals(0, car.getVersion());
    }

    public static void assertTopStudent(TopStudent topStudent, JsonObject rs) {
        try{
            Field studentVersion = Student.class.getDeclaredField("version");
            Field personVersion = Person.class.getDeclaredField("version");

            studentVersion.setAccessible(true);
            personVersion.setAccessible(true);

            assertEquals(topStudent.getNif(), rs.getInteger("nif").intValue());
            assertEquals(topStudent.getName(), rs.getString("name"));
            assertEquals(topStudent.getBirthday(), sdf.parse(rs.getString("birthday")).toInstant());
            assertEquals(topStudent.getVersion(), rs.getLong("Cversion").longValue());
            assertNotEquals(0, topStudent.getVersion());
            assertEquals(topStudent.getStudentNumber(), rs.getInteger("studentNumber").intValue());
            assertEquals(topStudent.getTopGrade(), rs.getInteger("topGrade").intValue());
            assertEquals(topStudent.getYear(), rs.getInteger("year").intValue());
            assertEquals(studentVersion.get(topStudent), rs.getLong("P1version"));
            assertEquals(personVersion.get(topStudent), rs.getLong("P2version"));
        } catch ( IllegalAccessException | NoSuchFieldException | ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertCompanyWithExternal(Company company, JsonObject rs, UnitOfWork unit) {
        assertEquals(company.getIdentityKey().getId(), rs.getInteger("id").intValue());
        assertEquals(company.getIdentityKey().getCid(), rs.getInteger("cid").intValue());
        assertEquals(company.getMotto(), rs.getString("motto"));
        assertEquals(company.getVersion(), rs.getLong("version").longValue());
        assertNotEquals(0, company.getVersion());

        CompletableFuture<List<Employee>> completableFuture = company.getEmployees().get();
        SQLConnection con = unit.getConnection().join();
        if(completableFuture != null) {
            List<Employee> employees = completableFuture.join();
            SqlUtils.<ResultSet>callbackToPromise(ar ->
                    con.queryWithParams("select id, name, companyId, companyCid, CAST(version as bigint) version from Employee where companyId = ? and companyCid = ?",
                            new JsonArray().add(company.getIdentityKey().getId()).add(company.getIdentityKey().getCid()), ar))
                    .thenAccept(resultSet -> resultSet.getRows(true).forEach(jo ->
                            assertEmployeeWithExternals(employees.remove(0), jo, unit)))
                    .join();
        }
        else {
            SqlUtils.<ResultSet>callbackToPromise(ar -> con.queryWithParams("select id, name, companyId, companyCid, CAST(version as bigint) version from Employee where companyId = ? and companyCid = ?",
                    new JsonArray().add(company.getIdentityKey().getId()).add(company.getIdentityKey().getCid()), ar))
                    .thenAccept(resultSet -> assertTrue(resultSet.getRows(true).isEmpty()))
                    .join();
        }
    }

    public static void assertCompany(Company company, JsonObject rs) {
        assertEquals(company.getIdentityKey().getId(), rs.getInteger("id").intValue());
        assertEquals(company.getIdentityKey().getCid(), rs.getInteger("cid").intValue());
        assertEquals(company.getMotto(), rs.getString("motto"));
        assertEquals(company.getVersion(), rs.getLong("Cversion").longValue());
        assertNotEquals(0, company.getVersion());
    }

    public static void assertEmployeeWithExternals(Employee employee, JsonObject rs, UnitOfWork unit) {
        assertEquals((int) employee.getIdentityKey(), rs.getInteger("id").intValue());
        assertEquals(employee.getName(), rs.getString("name"));
        assertEquals(employee.getVersion(), rs.getLong("version").longValue());
        //System.out.println(rs);
        CompletableFuture<Company> companyCompletableFuture = employee.getCompany().getForeignFunction().get();
        if(companyCompletableFuture != null){
            Company company = companyCompletableFuture.join();
            assertEquals(company.getIdentityKey().getId(), rs.getInteger("companyId").intValue());
            assertEquals(company.getIdentityKey().getCid(), rs.getInteger("companyCid").intValue());
        }
        else {
            assertNull(rs.getInteger("companyId".toUpperCase()));
            assertNull(rs.getInteger("companyCid".toUpperCase()));
        }
    }

    public static void assertEmployee(Employee employee, JsonObject rs) {
        assertEquals((int) employee.getIdentityKey(), rs.getInteger("id").intValue());
        assertEquals(employee.getName(), rs.getString("name"));
        assertEquals(employee.getVersion(), rs.getLong("version").longValue());
    }

    public static void assertBook(Book book, JsonObject rs, UnitOfWork unit){
        assertEquals((long) book.getIdentityKey(), rs.getLong("id").longValue());
        assertEquals(book.getName(), rs.getString("name"));
        assertEquals(book.getVersion(), rs.getLong("version").longValue());

        CompletableFuture<List<Author>> authors = book.getAuthors().get();
        if(authors != null) {
            Author author = authors.join().get(0);
            SqlUtils.<ResultSet>callbackToPromise(ar ->
                    unit.getConnection().join().queryWithParams("select id, name, CAST(version as bigint) version from Author where id = ?",
                            new JsonArray().add(author.getIdentityKey()), ar))
                    .thenAccept(resultSet -> assertAuthor(author, resultSet.getRows(true).get(0)))
                    .join();
        }
    }

    private static void assertAuthor(Author author, JsonObject rs) {
        assertEquals((long) author.getIdentityKey(), rs.getLong("id").longValue());
        assertEquals(author.getName(), rs.getString("name"));
        assertEquals(author.getVersion(), rs.getLong("version").longValue());
    }

    public static void assertStudent(Student student, JsonObject rs) {
        Field personVersion = null;
        try {
            personVersion = Person.class.getDeclaredField("version");
            personVersion.setAccessible(true);
            assertEquals(student.getNif(), rs.getInteger("nif").intValue());
            assertEquals(student.getName(), rs.getString("name"));
            //assertEquals(student.getBirthday(), rs.getDate("birthday"));
            assertEquals(student.getVersion(), rs.getLong("Cversion").longValue());
            assertNotEquals(0, student.getVersion());
            assertEquals(student.getStudentNumber(), rs.getInteger("studentNumber").intValue());
            assertEquals(personVersion.get(student), rs.getLong("P1version"));

        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertDog(Dog dog, JsonObject rs){
        assertEquals(dog.getIdentityKey().getName(), rs.getString("name"));
        assertEquals(dog.getIdentityKey().getRace(), rs.getString("race"));
        assertEquals(dog.getAge(), rs.getInteger("age").intValue());
    }

    //---------------------------ResultSets assertions-----------------------------------
    public static<U> void assertSingleRow(U object, String sql, JsonArray jsonArray, BiConsumer<U, JsonObject> assertConsumer, SQLConnection con) {
        ResultSet resultSet = SqlUtils.<ResultSet>callbackToPromise(ar -> con.queryWithParams(sql, jsonArray, ar)).join();
        if(resultSet.getRows(true).isEmpty())
            throw new DataMapperException("Object wasn't selected from the database");
        else
            assertConsumer.accept(object, resultSet.getRows(true).get(0));
    }

    public static<U> void assertMultipleRows(SQLConnection connection, List<U> list, String sql, BiConsumer<U, JsonObject> assertConsumer, int expectedRows){
        ResultSet resultSet = SqlUtils.<ResultSet>callbackToPromise(ar -> connection.query(sql, ar)).join();
        List<JsonObject> res = resultSet.getRows(true);
        assertEquals(expectedRows, res.size());
        assertConsumer.accept(list.get(0), res.get(0));
    }

    public static void assertNotFound(String sql, JsonArray jsonArray, SQLConnection con){
        ResultSet resultSet = SqlUtils.<ResultSet>callbackToPromise(ar -> con.queryWithParams(sql, jsonArray, ar)).join();
        assertTrue(resultSet.getRows(true).isEmpty());
    }
}
