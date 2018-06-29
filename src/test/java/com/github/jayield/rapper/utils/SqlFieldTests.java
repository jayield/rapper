package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.domainModel.Book;
import com.github.jayield.rapper.domainModel.Company;
import com.github.jayield.rapper.domainModel.Employee;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

public class SqlFieldTests {

    @Test
    public void testGetStrategy() throws NoSuchFieldException {
        Field authors = Book.class.getDeclaredField("authors");
        SqlField.SqlFieldExternal external = new SqlField.SqlFieldExternal(authors, "");
        assertEquals(PopulateWithExternalTable.class, external.getPopulateStrategy());

        Field employees = Company.class.getDeclaredField("employees");
        external = new SqlField.SqlFieldExternal(employees, "");
        assertEquals(PopulateMultiReference.class, external.getPopulateStrategy());

        Field company = Employee.class.getDeclaredField("company");
        external = new SqlField.SqlFieldExternal(company, "");
        assertEquals(PopulateSingleReference.class, external.getPopulateStrategy());
    }
}
