package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.EmbeddedId;

public class Rating implements DomainObject<Rating.RatingPK> {

    @EmbeddedId
    private final RatingPK ratingPK;
    private final long moderatorId; // can be null
    private final double ratingValue;
    private final boolean approved;
    private final long version;

    public Rating(long accountIdFrom,long accountIdTo, long moderatorId, double ratingValue, boolean approved, long version) {
        this.ratingPK = new RatingPK(accountIdFrom, accountIdTo);
        this.moderatorId = moderatorId;
        this.ratingValue = ratingValue;
        this.approved = approved;
        this.version = version;
    }

    public long getModeratorId() {
        return moderatorId;
    }

    public double getRatingValue() {
        return ratingValue;
    }

    public long getAccountIdTo() {
        return ratingPK.accountIdTo;
    }

    public long getAccountIdFrom() {
        return ratingPK.accountIdFrom;
    }

    public boolean isApproved() {
        return approved;
    }

    @Override
    public RatingPK getIdentityKey() {
        return ratingPK;
    }

    @Override
    public long getVersion() {
        return version;
    }

    public class RatingPK {
        private final long accountIdFrom;
        private final long accountIdTo;

        public RatingPK(long accountIdFrom, long accountIdTo) {
            this.accountIdFrom = accountIdFrom;
            this.accountIdTo = accountIdTo;
        }
    }
}
