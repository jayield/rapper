package com.github.jayield.rapper.mapper.externals;

import com.github.jayield.rapper.DomainObject;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class Foreign<T extends DomainObject<K>, K> {
    private final K foreignKey;
    private final Supplier<CompletableFuture<T>> foreignFunction;

    public Foreign(K foreignKey, Supplier<CompletableFuture<T>> foreignFunction) {
        this.foreignKey = foreignKey;
        this.foreignFunction = foreignFunction;
    }

    public K getForeignKey() {
        return foreignKey;
    }

    public CompletableFuture<T> getForeignObject() {
        return foreignFunction.get();
    }
}
