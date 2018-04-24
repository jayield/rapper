package org.github.isel.rapper.domainModel;

public class EmployeeJunior extends Employee {
    private int juniorsYears;
    private long version;

    public EmployeeJunior(){

    }

    public int getJuniorsYears() {
        return juniorsYears;
    }

    @Override
    public long getVersion() {
        return version;
    }
}
