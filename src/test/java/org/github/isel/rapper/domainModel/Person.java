package org.github.isel.rapper.domainModel;

import org.github.isel.rapper.DomainObject;
import org.github.isel.rapper.Id;

import java.sql.Date;

public class Person implements DomainObject<Integer> {
    @Id
    private final int nif;
    private final String name;
    private final Date birthday;
    private final long version;

    public Person(int nif, String name, Date birthday, long version) {
        this.nif = nif;
        this.name = name;
        this.birthday = birthday;
        this.version = version;
    }

    public Person(){
        this.nif = 0;
        this.name = null;
        this.birthday = null;
        this.version = 0;
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

    public Date getBirthday() {
        return birthday;
    }
}
