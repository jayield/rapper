package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.DomainObject;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class Foreign<T extends DomainObject<K>, K> {
    private final K foreignKey;
    private final Function<UnitOfWork, CompletableFuture<T>> foreignFunction;

    public Foreign(K foreignKey, Function<UnitOfWork, CompletableFuture<T>> foreignFunction) {
        this.foreignKey = foreignKey;
        this.foreignFunction = foreignFunction;
    }

    public K getForeignKey() {
        return foreignKey;
    }

    public Function<UnitOfWork, CompletableFuture<T>> getForeignFunction() {
        return foreignFunction;
    }
}
