package org.github.isel.rapper.domainModel;

import org.github.isel.rapper.ColumnName;
import org.github.isel.rapper.DomainObject;
import org.github.isel.rapper.EmbeddedId;

import java.util.List;
import java.util.function.Supplier;

public class Company implements DomainObject<Company.PrimaryKey> {
    @EmbeddedId
    private final PrimaryKey primaryKey;
    private final String motto;
    @ColumnName(name = {"companyId","companyCid"}, table = "CompanyEmployee", foreignName = {"employeeId"})
    private final Supplier<List<Employee>> allEmployees;
    @ColumnName(name = {"companyId","companyCid"})
    private final Supplier<List<Employee>> currentEmployees;
    private final long version;

    public Company(PrimaryKey primaryKey, String motto, Supplier<List<Employee>> allEmployees, Supplier<List<Employee>> currentEmployees, long version) {
        this.primaryKey = primaryKey;
        this.motto = motto;
        this.allEmployees = allEmployees;
        this.currentEmployees = currentEmployees;
        this.version = version;
    }

    public Company() {
        primaryKey = null;
        motto = null;
        allEmployees = null;
        currentEmployees = null;
        version = 0;
    }

    public Supplier<List<Employee>> getAllEmployees() {
        return allEmployees;
    }

    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public String getMotto() {
        return motto;
    }

    public Supplier<List<Employee>> getCurrentEmployees() {
        return currentEmployees;
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
