package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.annotations.Id;
import com.github.jayield.rapper.annotations.Version;

import java.time.Instant;

public class Person implements DomainObject<Integer> {
    @Id
    private final int nif;
    private final String name;
    private final Instant birthday;
    @Version
    private final long version;

    public Person(int nif, String name, Instant birthday, long version) {
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
                ", version=" + version +
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

    public Instant getBirthday() {
        return birthday;
    }
}
