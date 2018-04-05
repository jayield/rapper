package org.github.isel.rapper;

import java.sql.Timestamp;

public class Person implements DomainObject<Integer> {
    @Id(isIdentity = true)
    private final int nif;
    private final String name;
    private final Timestamp birthday;
    private final long version;

    public Person(int nif, String name, Timestamp birthday, long version) {
        this.nif = nif;
        this.name = name;
        this.birthday = birthday;
        this.version = version;
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
        return version;
    }

    public int getNif() {
        return nif;
    }

    public String getName() {
        return name;
    }

    public Timestamp getBirthday() {
        return birthday;
    }
}
