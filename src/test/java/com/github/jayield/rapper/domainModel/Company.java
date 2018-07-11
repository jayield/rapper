package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.ColumnName;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.EmbeddedId;
import com.github.jayield.rapper.Version;
import com.github.jayield.rapper.utils.EmbeddedIdClass;
import com.github.jayield.rapper.utils.UnitOfWork;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class Company implements DomainObject<Company.PrimaryKey> {
    @EmbeddedId
    private final PrimaryKey primaryKey;
    private final String motto;
    @ColumnName(foreignName = {"companyId", "companyCid"})
    private final Function<UnitOfWork, CompletableFuture<List<Employee>>> employees;
    @Version
    private final long version;

    public Company(PrimaryKey primaryKey, String motto, Function<UnitOfWork, CompletableFuture<List<Employee>>> employees, long version) {
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

    public Function<UnitOfWork, CompletableFuture<List<Employee>>> getEmployees() {
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

    public static class PrimaryKey extends EmbeddedIdClass {
        private final Integer id;
        private final Integer cid;

        public PrimaryKey(int id, int cid) {
            super(id, cid);
            this.id = id;
            this.cid = cid;
        }

        public PrimaryKey() {
            super();
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

    @Override
    public String toString() {
        return "Company{" +
                "primaryKey=" + primaryKey +
                ", motto='" + motto + '\'' +
                ", employees=" + employees +
                ", version=" + version +
                '}';
    }
}
