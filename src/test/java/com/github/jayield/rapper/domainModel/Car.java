package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.EmbeddedId;

public class Car implements DomainObject<Car.PrimaryPk> {
    @EmbeddedId
    private final PrimaryPk pk;
    private final String brand;
    private final String model;
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

    public static class PrimaryPk{
        private final int owner;
        private final String plate;

        public PrimaryPk(int owner, String plate) {
            this.owner = owner;
            this.plate = plate;
        }

        public PrimaryPk() {
            owner = 1;
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
