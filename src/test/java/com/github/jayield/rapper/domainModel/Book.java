package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.annotations.ColumnName;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.annotations.Id;
import com.github.jayield.rapper.annotations.Version;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class Book implements DomainObject<Long> {

    @Id(isIdentity = true)
    private long id;
    private String name;
    @Version
    private long version;

    @ColumnName(foreignName = "bookId", table = "BookAuthor", externalName = "authorId")
    private Supplier<CompletableFuture<List<Author>>> authors;

    public Book(long id, String name, long version, Supplier<CompletableFuture<List<Author>>> authors) {
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

    public Supplier<CompletableFuture<List<Author>>> getAuthors() {
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
