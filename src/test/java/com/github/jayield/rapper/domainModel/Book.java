package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.ColumnName;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.Id;
import com.github.jayield.rapper.Version;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Book implements DomainObject<Long> {

    @Id(isIdentity = true)
    private long id;
    private String name;
    @Version
    private long version;

    @ColumnName(foreignName = "bookId", table = "BookAuthor", externalName = "authorId")
    private CompletableFuture<List<Author>> authors;

    public Book(long id, String name, long version, CompletableFuture<List<Author>> authors) {
        this.id = id;
        this.name = name;
        this.version = version;
        this.authors = authors;
    }

    public Book() {
    }

    public String getName() {
        return name;
    }

    public CompletableFuture<List<Author>> getAuthors() {
        return authors;
    }

    @Override
    public Long getIdentityKey() {
        return id;
    }

    @Override
    public long getVersion() {
        return version;
    }
}
