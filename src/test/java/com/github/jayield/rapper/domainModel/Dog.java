package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.annotations.EmbeddedId;
import com.github.jayield.rapper.utils.EmbeddedIdClass;

public class Dog implements DomainObject<Dog.DogPK> {

    @EmbeddedId
    private DogPK pk;
    private int age;

    public Dog(DogPK pk, int age) {
        this.pk = pk;
        this.age = age;
    }

    public Dog() {
    }

    public DogPK getPk() {
        return pk;
    }

    public void setPk(DogPK pk) {
        this.pk = pk;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public DogPK getIdentityKey() {
        return pk;
    }

    @Override
    public long getVersion() {
        return 0;
    }

    public static class DogPK extends EmbeddedIdClass {
        private String name;
        private String race;

        public DogPK(String name, String race) {
            super(name, race);
            this.name = name;
            this.race = race;
        }

        public DogPK() {
            super();
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getRace() {
            return race;
        }

        public void setRace(String race) {
            this.race = race;
        }
    }
}
