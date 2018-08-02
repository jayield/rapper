package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.annotations.EmbeddedId;
import com.github.jayield.rapper.annotations.Version;

public class Car implements DomainObject<CarKey> {
    @EmbeddedId
    private final CarKey pk;
    private final String brand;
    private final String model;
    @Version
    private final long version;

    public Car(int owner, String plate, String brand, String model, long version) {
        this.pk = new CarKey(owner, plate);
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
    public CarKey getIdentityKey() {
        return pk;
    }

    @Override
    public long getVersion() {
        return version;
    }
}
