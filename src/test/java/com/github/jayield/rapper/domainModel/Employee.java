package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.ColumnName;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.Id;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Employee implements DomainObject<Integer> {
    @Id(isIdentity = true)
    private final int id;
    private final String name;
    private final int companyId;
    private final int companyCid;
    private final long version;

    public Employee(int id, String name, int companyId, int companyCid, long version) {
        this.id = id;
        this.name = name;
        this.companyId = companyId;
        this.companyCid = companyCid;
        this.version = version;
    }

    public Employee() {
        id = 0;
        name = null;
        companyId = 0;
        companyCid = 0;
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

    @Override
    public Integer getIdentityKey() {
        return id;
    }

    @Override
    public long getVersion() {
        return version;
    }
}
