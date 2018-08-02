package com.github.jayield.rapper.mapper.externals;

import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.unitofwork.UnitOfWork;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

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

    public CompletableFuture<T> getForeignObject(UnitOfWork unit) {
        return foreignFunction.apply(unit);
    }
}
