package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.ColumnName;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.EmbeddedId;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Company implements DomainObject<Company.PrimaryKey> {
    @EmbeddedId
    private final PrimaryKey primaryKey;
    private final String motto;
    @ColumnName(foreignName = {"companyId", "companyCid"})
    private final CompletableFuture<List<Employee>> employees;
    private final long version;

    public Company(PrimaryKey primaryKey, String motto, CompletableFuture<List<Employee>> employees, long version) {
        this.primaryKey = primaryKey;
        this.motto = motto;
        this.employees = employees;
        this.version = version;
    }

    public Company() {
        primaryKey = null;
        motto = null;
        employees = null;
        version = 0;
    }

    public String getMotto() {
        return motto;
    }

    public CompletableFuture<List<Employee>> getEmployees() {
        return employees;
    }

    @Override
    public PrimaryKey getIdentityKey() {
        return primaryKey;
    }

    @Override
    public long getVersion() {
        return version;
    }

    public static class PrimaryKey {
        private final int id;
        private final int cid;

        public PrimaryKey(int id, int cid) {
            this.id = id;
            this.cid = cid;
        }

        public PrimaryKey() {
            id = 0;
            cid = 0;
        }

        public int getId() {
            return id;
        }

        public int getCid() {
            return cid;
        }
    }
}
