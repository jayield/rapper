package org.github.isel.rapper;

import java.sql.Date;
import java.sql.Timestamp;

public class TopStudent extends Student {
    private final int topGrade;
    private final int year;

    public TopStudent(int nif, String name, Date birthday, long version, int studentNumber, int topGrade, int year) {
        super(nif, name, birthday, version, studentNumber);
        this.topGrade = topGrade;
        this.year = year;
    }

    public TopStudent(){
        super(0, null, null, 0, 0);
        this.topGrade = 0;
        this.year = 0;
    }

    public int getTopGrade() {
        return topGrade;
    }

    public int getYear() {
        return year;
    }
}
