package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.annotations.Version;

import java.time.Instant;

public class Student extends Person {

    private final int studentNumber;
    @Version
    private final long version;

    public Student(int nif, String name, Instant birthday, long personVersion, int studentNumber, long version) {
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
