package com.github.jayield.rapper.utils;

public class Pair<T, R> {
    final T key;
    final R value;

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
