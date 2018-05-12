package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.utils.EmbeddedIdClass;

public class CarKey extends EmbeddedIdClass {
    private final int owner;
    private final String plate;

    public CarKey(int owner, String plate) {
        super(owner, plate);
        this.owner = owner;
        this.plate = plate;
    }

    public CarKey() {
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