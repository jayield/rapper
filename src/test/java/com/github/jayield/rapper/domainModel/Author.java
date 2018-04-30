package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.ColumnName;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.Id;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Author implements DomainObject<Long> {

    @Id(isIdentity = true)
    private long id;
    private String name;
    private long version;
    @ColumnName(foreignName = "authorId", table = "BookAuthor", externalName = "bookId")
    private CompletableFuture<List<Book>> books;

    public Author() {
    }

    public String getName() {
        return name;
    }

    public CompletableFuture<List<Book>> getBooks() {
        return books;
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
