package com.github.jayield.rapper.utils;

public class Pair<T, R> {
    private final T key;
    private final R value;

    public Pair(T key, R value) {
        this.key = key;
        this.value = value;
    }

    public T getKey() {
        return key;
    }

    public R getValue() {
        return value;
    }
}
