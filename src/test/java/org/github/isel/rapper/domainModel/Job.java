package org.github.isel.rapper.domainModel;

import org.github.isel.rapper.DomainObject;
import org.github.isel.rapper.Id;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class Job implements DomainObject<Long> {
    @Id(isIdentity = true)
    private long jobId;
    private final long accountID;
    private final String address;
    private final int wage;
    private final String description;
    private final String schedule;
    private final Timestamp offerBeginDate;
    private final Timestamp offerEndDate;
    private final String offerType; //TODO change to enum
    private final long version;

    public Job(
            long id,
            long accountID,
            String address,
            int wage,
            String description,
            String schedule,
            Timestamp offerBeginDate,
            Timestamp offerEndDate,
            String offerType,
            long version
    ) {
        this.jobId = id;
        this.accountID = accountID;
        this.address = address;
        this.wage = wage;
        this.description = description;
        this.schedule = schedule;
        this.offerBeginDate = offerBeginDate;
        this.offerEndDate = offerEndDate;
        this.offerType = offerType;
        this.version = version;
    }

    public long getAccountID() {
        return accountID;
    }

    public int getWage() {
        return wage;
    }

    public String getDescription() {
        return description;
    }

    public String getSchedule() {
        return schedule;
    }

    public Timestamp getOfferBeginDate() {
        return offerBeginDate;
    }

    public Timestamp getOfferEndDate() {
        return offerEndDate;
    }

    public String getOfferType() {
        return offerType;
    }

    public String getAddress() {
        return address;
    }

    @Override
    public Long getIdentityKey() {
        return jobId;
    }

    @Override
    public long getVersion() {
        return version;
    }
}
