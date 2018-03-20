package rapper;

import java.sql.Date;

public class Student extends Person {

    private final int studentNumber;

    public Student(int nif, String name, Date birthday, int studentNumber) {
        super(nif, name, birthday);
        this.studentNumber = studentNumber;
    }
}
