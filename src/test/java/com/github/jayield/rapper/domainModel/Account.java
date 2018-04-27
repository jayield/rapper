package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.ColumnName;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.Id;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Account implements DomainObject<Long> {
    @Id(isIdentity = true)
    protected long accountID;
    protected final String email;
    protected final String password;
    protected final float rating;
    protected final long version;

    @ColumnName(name = "accountId")
    protected final CompletableFuture<List<Job>> offeredJobs;
    @ColumnName(name = "accountId")
    protected final CompletableFuture<List<Comment>> comments;
    @ColumnName(name = "accountId")
    protected final CompletableFuture<List<Chat>> chats;
    @ColumnName(name = "accountId")
    protected final CompletableFuture<List<Rating>> ratings;
    @ColumnName(name = "accountId")
    protected final CompletableFuture<List<Account>> following;

    protected Account(
            long accountID,
            String email,
            String password,
            float rating,
            long version,
            CompletableFuture<List<Job>> offeredJobs,
            CompletableFuture<List<Comment>> comments,
            CompletableFuture<List<Chat>> chats,
            CompletableFuture<List<Rating>> ratings,
            CompletableFuture<List<Account>> following
    ){
        this.accountID = accountID;
        this.email = email;
        this.password = password;
        this.rating = rating;
        this.offeredJobs = offeredJobs;
        this.chats = chats;
        this.ratings = ratings;
        this.comments = comments;
        this.following = following;
        this.version = version;
    }

    public Account(){

        email = null;
        password = null;
        rating = 0;
        version = 0;
        offeredJobs = null;
        comments = null;
        chats = null;
        ratings = null;
        following = null;
    }

    public String getEmail() {
        return email;
    }

    public String getPassword() {
        return password;
    }

    public float getRating() {
        return rating;
    }

    public CompletableFuture<List<Job>> getOfferedJobs() {
        return offeredJobs;
    }

    public CompletableFuture<List<Account>> getFollowing() {
        return following;
    }

    public CompletableFuture<List<Comment>> getComments() {
        return comments;
    }

    public CompletableFuture<List<Chat>> getChats() {
        return chats;
    }

    public CompletableFuture<List<Rating>> getRatings() {
        return ratings;
    }

    @Override
    public Long getIdentityKey() {
        return accountID;
    }

    @Override
    public long getVersion() {
        return version;
    }
}
