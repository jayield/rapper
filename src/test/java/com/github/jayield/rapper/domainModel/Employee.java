package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.ColumnName;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.Id;
import com.github.jayield.rapper.Version;
import com.github.jayield.rapper.utils.Foreign;

public class Employee implements DomainObject<Integer> {
    @Id(isIdentity = true)
    private final int id;
    private final String name;
    @Version
    private final long version;
    @ColumnName(name = {"companyId", "companyCid"})
    private final Foreign<Company, Company.PrimaryKey> company;

    public Employee(int id, String name, long version, Foreign<Company, Company.PrimaryKey> company) {
        this.id = id;
        this.name = name;
        this.version = version;
        this.company = company;
    }

    public Employee() {
        id = 0;
        name = null;
        version = 0;
        company = null;
    }

    public String getName() {
        return name;
    }

    public Foreign<Company, Company.PrimaryKey> getCompany() {
        return company;
    }

    @Override
    public Integer getIdentityKey() {
        return id;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "Employee: {id=" + id + ", name=" + name + ", version=" + version + "}";
    }
}
