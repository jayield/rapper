package rapper;

import org.github.isel.rapper.DomainObject;
import org.github.isel.rapper.Id;

import java.sql.Date;

public class Person implements DomainObject<Integer> {
    @Id
    private final int nif;
    private final String name;
    private final Date birthday;

    public Person(int nif, String name, Date birthday) {
        this.nif = nif;
        this.name = name;
        this.birthday = birthday;
    }

    @Override
    public String toString() {
        return "Person{" +
                "nif=" + nif +
                ", name='" + name + '\'' +
                ", birthday=" + birthday +
                '}';
    }

    @Override
    public Integer getIdentityKey() {
        return nif;
    }

    @Override
    public long getVersion() {
        return 0;
    }

    public int getNif() {
        return nif;
    }

    public String getName() {
        return name;
    }

    public Date getBirthday() {
        return birthday;
    }
}
