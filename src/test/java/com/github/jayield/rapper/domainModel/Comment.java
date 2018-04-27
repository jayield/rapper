package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.Id;

import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Comment implements DomainObject<Long> {
    @Id(isIdentity = true)
    private long commentID;
    private final long accountIdFrom;
    private final long accountIdTo;
    private final long parentCommendID;
    private final Timestamp date;
    private final String text;
    private final boolean status; //moderated or not

    private final CompletableFuture<List<Comment>> replies;
    private final long version;

    public Comment(long commentID, long accountIdFrom, long accountIdTo, long parentCommendID, Timestamp date, String text, boolean status, CompletableFuture<List<Comment>> replies, long version){
        this.commentID = commentID;
        this.accountIdFrom = accountIdFrom;
        this.accountIdTo = accountIdTo;
        this.parentCommendID = parentCommendID;
        this.date = date;
        this.text = text;
        this.status = status;
        this.replies = replies;
        this.version = version;
    }

    public long getCommentID() {
        return commentID;
    }

    public long getAccountIdFrom() {
        return accountIdFrom;
    }

    public long getAccountIdTo() {
        return accountIdTo;
    }

    public long getMainCommendID() {
        return parentCommendID;
    }

    public Timestamp getDate() {
        return date;
    }

    public String getText() {
        return text;
    }

    public boolean getStatus() {
        return status;
    }

    public CompletableFuture<List<Comment>> getReplies() {
        return replies;
    }

    @Override
    public Long getIdentityKey() {
        return commentID;
    }

    @Override
    public long getVersion() {
        return version;
    }
}
