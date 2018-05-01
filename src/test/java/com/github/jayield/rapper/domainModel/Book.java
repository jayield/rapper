package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.ColumnName;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.Id;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Book implements DomainObject<Long> {

    @Id(isIdentity = true)
    private long id;
    private String name;
    private long version;

    @ColumnName(foreignName = "bookId", table = "BookAuthor", externalName = "authorId")
    private CompletableFuture<List<Author>> authors;

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
