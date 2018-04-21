package org.github.isel.rapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DBStatements {
    public static void populateDB(Connection con) throws SQLException {
        PreparedStatement stmt = con.prepareStatement("insert into Person (nif, name, birthday) values (321, 'Jose', '1996-6-2')");
        stmt.execute();
        stmt = con.prepareStatement("insert into Person (nif, name, birthday) values (454, 'Nuno', '1996-4-2')");
        stmt.execute();
        stmt = con.prepareStatement("insert into Car (owner, plate, brand, model) values (2, '23we45', 'Mitsubishi', 'lancer')");
        stmt.execute();
        stmt = con.prepareStatement("insert into Student (nif, studentNumber) values (454, 3)");
        stmt.execute();
        stmt = con.prepareStatement("insert into TopStudent (nif, topGrade, year) values (454, 20, 2017)");
        stmt.execute();
        stmt = con.prepareStatement("insert into Company (id, cid, motto) values (1, 1, 'Living la vida loca')");
        stmt.execute();
        stmt = con.prepareStatement("insert into Employee (name, companyId, companyCid) VALUES ('Bob', 1, 1)");
        stmt.execute();
        stmt = con.prepareStatement("insert into Employee (name, companyId, companyCid) VALUES ('Charles', 1, 1)");
        stmt.execute();
        con.commit();
    }

    public static void deleteDB(Connection con) throws SQLException {
        PreparedStatement stmt = con.prepareStatement("delete from CompanyEmployee");
        stmt.execute();
        stmt = con.prepareStatement("delete from Employee");
        stmt.execute();
        stmt = con.prepareStatement("delete from TopStudent");
        stmt.execute();
        stmt = con.prepareStatement("delete from Student");
        stmt.execute();
        stmt = con.prepareStatement("delete from Car");
        stmt.execute();
        stmt = con.prepareStatement("delete from Person");
        stmt.execute();
        stmt = con.prepareStatement("delete from Company");
        stmt.execute();
        con.commit();
    }

    public static void createTables(Connection con) throws SQLException {
        PreparedStatement stmt = con.prepareStatement("CREATE TABLE IF NOT EXISTS Person (\n" +
                "\tnif int,\n" +
                "\t[name] nvarchar(50),\n" +
                "\tbirthday date,\n" +
                "\tversion rowversion\n" +
                ")");
        stmt.execute();
        stmt = con.prepareStatement("CREATE TABLE IF NOT EXISTS Car (\n" +
                "  owner int,\n" +
                "  plate varchar(6),\n" +
                "  brand varchar(20),\n" +
                "  model varchar(20),\n" +
                "  version rowversion,\n" +
                "\n" +
                "  PRIMARY KEY (owner, plate)\n" +
                ")");
        stmt.execute();
        stmt = con.prepareStatement("CREATE TABLE IF NOT EXISTS Student (\n" +
                "  nif int references Person,\n" +
                "  studentNumber int,\n" +
                "  version rowversion,\n" +
                "\n" +
                "  PRIMARY KEY (nif)\n" +
                ")");
        stmt.execute();
        stmt = con.prepareStatement("CREATE TABLE IF NOT EXISTS TopStudent (\n" +
                "  nif int references Student,\n" +
                "  topGrade int,\n" +
                "  year int,\n" +
                "  version rowversion,\n" +
                "\n" +
                "  PRIMARY KEY (nif)\n" +
                ")");
        stmt.execute();
        stmt = con.prepareStatement("CREATE TABLE IF NOT EXISTS Company (\n" +
                "  id int,\n" +
                "  cid int,\n" +
                "  motto varchar(20),\n" +
                "  version rowversion,\n" +
                "\n" +
                "  PRIMARY KEY (id, cid)\n" +
                ")");
        stmt.execute();
        stmt = con.prepareStatement("CREATE TABLE IF NOT EXISTS Employee (\n" +
                "  id int identity,\n" +
                "  name varchar(20),\n" +
                "  companyId int,\n" +
                "  companyCid int,\n" +
                "  version rowversion,\n" +
                "\n" +
                "  PRIMARY KEY (id),\n" +
                "  FOREIGN KEY (companyId, companyCid) references Company (id, cid)\n" +
                ")");
        stmt.execute();
        stmt = con.prepareStatement("CREATE TABLE IF NOT EXISTS CompanyEmployee (\n" +
                "  employeeId int references Employee,\n" +
                "  companyId int,\n" +
                "  companyCid int,\n" +
                "  version rowversion,\n" +
                "\n" +
                "  PRIMARY KEY (employeeId, companyId, companyCid),\n" +
                "  FOREIGN KEY (companyId, companyCid) REFERENCES Company (id, cid)\n" +
                ")");
        stmt.execute();
        con.commit();
    }
}
