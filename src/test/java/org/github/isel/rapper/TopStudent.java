package org.github.isel.rapper;

import java.sql.Date;
import java.sql.Timestamp;

public class TopStudent extends Student {
    public TopStudent(int nif, String name, Date birthday, long version, int studentNumber) {
        super(nif, name, birthday, version, studentNumber);
    }
}
