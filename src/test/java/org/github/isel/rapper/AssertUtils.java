package org.github.isel.rapper;

import org.github.isel.rapper.domainModel.*;
import org.github.isel.rapper.utils.UnitOfWork;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.Assert.*;

public class AssertUtils {
    //---------------------Domain Objects assertions----------------------------------
    public static void assertPerson(Person person, ResultSet rs) {
        try {
            assertEquals(person.getNif(), rs.getInt("nif"));
            assertEquals(person.getName(), rs.getString("name"));
            assertEquals(person.getBirthday(), rs.getDate("birthday"));
            assertEquals(person.getVersion(), rs.getLong("version"));
            assertNotEquals(0, person.getVersion());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertCar(Car car, ResultSet rs) {
        try {
            assertEquals(car.getIdentityKey().getOwner(), rs.getInt("owner"));
            assertEquals(car.getIdentityKey().getPlate(), rs.getString("plate"));
            assertEquals(car.getBrand(), rs.getString("brand"));
            assertEquals(car.getModel(), rs.getString("model"));
            assertEquals(car.getVersion(), rs.getLong("version"));
            assertNotEquals(0, car.getVersion());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertTopStudent(TopStudent topStudent, ResultSet rs) {
        try{
            Field studentVersion = Student.class.getDeclaredField("version");
            Field personVersion = Person.class.getDeclaredField("version");

            studentVersion.setAccessible(true);
            personVersion.setAccessible(true);

            assertEquals(topStudent.getNif(), rs.getInt("nif"));
            assertEquals(topStudent.getName(), rs.getString("name"));
            assertEquals(topStudent.getBirthday(), rs.getDate("birthday"));
            assertEquals(topStudent.getVersion(), rs.getLong("Cversion"));
            assertNotEquals(0, topStudent.getVersion());
            assertEquals(topStudent.getStudentNumber(), rs.getInt("studentNumber"));
            assertEquals(topStudent.getTopGrade(), rs.getInt("topGrade"));
            assertEquals(topStudent.getYear(), rs.getInt("year"));
            assertEquals(studentVersion.get(topStudent), rs.getLong("P1version"));
            assertEquals(personVersion.get(topStudent), rs.getLong("P2version"));
        } catch (SQLException | IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertCompany(Company company, ResultSet rs, UnitOfWork current) {
        try {
            assertEquals(company.getIdentityKey().getId(), rs.getInt("id"));
            assertEquals(company.getIdentityKey().getCid(), rs.getInt("cid"));
            assertEquals(company.getMotto(), rs.getString("motto"));
            assertEquals(company.getVersion(), rs.getLong("version"));
            assertNotEquals(0, company.getVersion());

            List<Employee> employees = company.getCurrentEmployees().get();
            PreparedStatement ps = current.getConnection()
                    .prepareStatement("select id, name, companyId, companyCid, CAST(version as bigint) version from Employee where companyId = ? and companyCid = ?");
            ps.setInt(1, company.getIdentityKey().getId());
            ps.setInt(2, company.getIdentityKey().getCid());
            rs = ps.executeQuery();

            while (rs.next()) {
                assertEmployee(employees.remove(0), rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertEmployee(Employee employee, ResultSet rs) {
        try {
            assertEquals(employee.getId(), rs.getInt("id"));
            assertEquals(employee.getName(), rs.getString("name"));
            assertEquals(employee.getCompanyId(), rs.getInt("companyId"));
            assertEquals(employee.getCompanyCid(), rs.getInt("companyCid"));
            assertEquals(employee.getVersion(), rs.getLong("version"));
            assertNotEquals(0, employee.getVersion());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertStudent(Student student, ResultSet rs) {
        try{
            Field personVersion = Person.class.getDeclaredField("version");

            personVersion.setAccessible(true);

            assertEquals(student.getNif(), rs.getInt("nif"));
            assertEquals(student.getName(), rs.getString("name"));
            //assertEquals(student.getBirthday(), rs.getDate("birthday"));
            assertEquals(student.getVersion(), rs.getLong("Cversion"));
            assertNotEquals(0, student.getVersion());
            assertEquals(student.getStudentNumber(), rs.getInt("studentNumber"));
            assertEquals(personVersion.get(student), rs.getLong("P1version"));
        } catch (SQLException | IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertEmployeeJunior(EmployeeJunior employeeJunior, ResultSet rs) {
        try {
            Field employeeVersion = Employee.class.getDeclaredField("version");

            employeeVersion.setAccessible(true);

            assertEquals(employeeJunior.getId(), rs.getInt("id"));
            assertEquals(employeeJunior.getName(), rs.getString("name"));
            assertEquals(employeeJunior.getCompanyId(), rs.getInt("companyId"));
            assertEquals(employeeJunior.getCompanyCid(), rs.getInt("companyCid"));
            assertEquals(employeeVersion.get(employeeJunior), rs.getLong("P1version"));
            assertEquals(employeeJunior.getVersion(), rs.getLong("Cversion"));
            assertEquals(employeeJunior.getJuniorsYears(), rs.getLong("juniorsYears"));
            assertNotEquals(0, employeeJunior.getVersion());
        } catch (SQLException | NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    //---------------------------ResultSets assertions-----------------------------------
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

    public static void assertNotFound(String sql, Consumer<PreparedStatement> prepareStatement){
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
