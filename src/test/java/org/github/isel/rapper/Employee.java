package org.github.isel.rapper;

import java.util.List;
import java.util.function.Supplier;

public class Employee implements DomainObject<Integer>{
    @Id(isIdentity = true)
    private final int id;
    private final String name;
    private final int companyId;
    private final int companyCid;
    @ColumnName(name = "employeeId", table = "CompanyEmployee")
    private final Supplier<List<Company>> companies;
    private final long version;

    public Employee(int id, String name, int companyId, int companyCid, long version, Supplier<List<Company>> companies) {
        this.id = id;
        this.name = name;
        this.companyId = companyId;
        this.companyCid = companyCid;
        this.companies = companies;
        this.version = version;
    }

    public Employee() {
        id = 0;
        name = null;
        companyId = 0;
        companyCid = 0;
        companies = null;
        version = 0;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getCompanyId() {
        return companyId;
    }

    public int getCompanyCid() {
        return companyCid;
    }

    public Supplier<List<Company>> getCompanies() {
        return companies;
    }

    @Override
    public Integer getIdentityKey() {
        return id;
    }

    @Override
    public long getVersion() {
        return version;
    }
}
