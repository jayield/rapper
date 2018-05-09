package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.EmbeddedId;
import com.github.jayield.rapper.Version;
import com.github.jayield.rapper.utils.EmbeddedIdClass;

public class Car implements DomainObject<Car.PrimaryPk> {
    @EmbeddedId
    private final PrimaryPk pk;
    private final String brand;
    private final String model;
    @Version
    private final long version;

    public Car(int owner, String plate, String brand, String model, long version) {
        this.pk = new PrimaryPk(owner, plate);
        this.brand = brand;
        this.model = model;
        this.version = version;
    }

    public Car(){
        this.pk = null;
        this.brand = null;
        this.model = null;
        this.version = 0;
    }

    public String getBrand() {
        return brand;
    }

    public String getModel() {
        return model;
    }

    @Override
    public PrimaryPk getIdentityKey() {
        return pk;
    }

    @Override
    public long getVersion() {
        return version;
    }

    public static class PrimaryPk extends EmbeddedIdClass {
        private final int owner;
        private final String plate;

        public PrimaryPk(int owner, String plate) {
            super(owner, plate);
            this.owner = owner;
            this.plate = plate;
        }

        public PrimaryPk() {
            super();
            owner = 0;
            plate = null;
        }

        public int getOwner() {
            return owner;
        }

        public String getPlate() {
            return plate;
        }
    }
}
