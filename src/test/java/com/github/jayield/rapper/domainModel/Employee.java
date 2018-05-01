package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.ColumnName;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.Id;

import java.util.concurrent.CompletableFuture;

public class Employee implements DomainObject<Integer> {
    @Id(isIdentity = true)
    private final int id;
    private final String name;
    private final long version;
    @ColumnName(name = {"companyId", "companyCid"})
    private final CompletableFuture<Company> company;

    public Employee(int id, String name, long version, CompletableFuture<Company> company) {
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

    public CompletableFuture<Company> getCompany() {
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
}
