package org.github.isel.rapper;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface Mapper<T, K> {
    CompletableFuture<T> getById(K id);
    CompletableFuture<List<T>> getAll();
    void insert(T obj);
    void update(T obj);
    void delete(T obj);
}
