package org.github.isel.rapper;

import java.sql.Date;
import java.sql.Timestamp;

public class Student extends Person {

    private final int studentNumber;

    public Student(int nif, String name, Timestamp birthday, long version, int studentNumber) {
        super(nif, name, birthday, version);
        this.studentNumber = studentNumber;
    }
}
