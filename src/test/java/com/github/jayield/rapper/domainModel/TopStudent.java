package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.annotations.Version;

import java.time.Instant;

public class TopStudent extends Student {
    private final int topGrade;
    private final int year;
    @Version
    private final long version;

    public TopStudent(int nif, String name, Instant birthday, long studentVersion, int studentNumber, int topGrade, int year, long version, long personVersion) {
        super(nif, name, birthday, personVersion, studentNumber, studentVersion);
        this.topGrade = topGrade;
        this.year = year;
        this.version = version;
    }

    public TopStudent(){
        topGrade = 0;
        year = 0;
        version = 0;
    }

    public int getTopGrade() {
        return topGrade;
    }

    public int getYear() {
        return year;
    }

    @Override
    public long getVersion() {
        return version;
    }
}
