package com.github.jayield.rapper.sql;

import com.github.jayield.rapper.domainModel.Author;
import com.github.jayield.rapper.domainModel.Book;
import com.github.jayield.rapper.domainModel.Company;
import com.github.jayield.rapper.domainModel.Employee;
import com.github.jayield.rapper.mapper.externals.PopulateMultiReference;
import com.github.jayield.rapper.mapper.externals.PopulateSingleReference;
import com.github.jayield.rapper.mapper.externals.PopulateWithExternalTable;
import com.github.jayield.rapper.sql.SqlFieldExternal;
import org.junit.Test;

import java.lang.reflect.Field;

import static com.github.jayield.rapper.sql.SqlField.*;
import static org.junit.Assert.assertEquals;

public class SqlFieldTests {

    @Test
    public void testGetStrategy() throws NoSuchFieldException {
        Field authors = Book.class.getDeclaredField("authors");
        SqlFieldExternal external = new SqlFieldExternal(authors, "");
        assertEquals(PopulateWithExternalTable.class, external.getPopulateStrategy());

        Field employees = Company.class.getDeclaredField("employees");
        external = new SqlFieldExternal(employees, "");
        assertEquals(PopulateMultiReference.class, external.getPopulateStrategy());

        Field company = Employee.class.getDeclaredField("company");
        external = new SqlFieldExternal(company, "");
        assertEquals(PopulateSingleReference.class, external.getPopulateStrategy());
    }
    
    @Test
    public void testToSqlFieldExternal() throws NoSuchFieldException {
        Field field = Company.class.getDeclaredField("employees");
        SqlFieldExternal external = new SqlFieldExternal(field, "");
        assertEquals(Employee.class, external.getDomainObjectType());

        field = Author.class.getDeclaredField("books");
        external = new SqlFieldExternal(field, "");
        assertEquals(Book.class, external.getDomainObjectType());

        field = Employee.class.getDeclaredField("company");
        external = new SqlFieldExternal(field, "");
        assertEquals(Company.class, external.getDomainObjectType());
    }
}
