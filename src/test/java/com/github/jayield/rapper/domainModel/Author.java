package com.github.jayield.rapper.domainModel;

import com.github.jayield.rapper.annotations.ColumnName;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.annotations.Id;
import com.github.jayield.rapper.annotations.Version;
import com.github.jayield.rapper.unitofwork.UnitOfWork;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

public class Author implements DomainObject<Long> {

    @Id(isIdentity = true)
    private long id;
    private String name;
    @Version
    private long version;
    @ColumnName(foreignName = "authorId", table = "BookAuthor", externalName = "bookId")
    private Function<UnitOfWork, CompletableFuture<List<Book>>> books;

    public String getName() {
        return name;
    }

    public Function<UnitOfWork, CompletableFuture<List<Book>>> getBooks() {
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

    @Override
    public String toString() {
        return "Author{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", version=" + version +
                ", books=" + books +
                '}';
    }
}
