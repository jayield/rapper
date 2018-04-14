package org.github.isel.rapper.domainModel;

import java.sql.Date;

public class Student extends Person {

    private final int studentNumber;
    private final long version;

    public Student(int nif, String name, Date birthday, long personVersion, int studentNumber, long version) {
        super(nif, name, birthday, personVersion);
        this.studentNumber = studentNumber;
        this.version = version;
    }

    public Student(){
        studentNumber = 0;
        version = 0;
    }

    public int getStudentNumber() {
        return studentNumber;
    }

    @Override
    public long getVersion() {
        return version;
    }
}
