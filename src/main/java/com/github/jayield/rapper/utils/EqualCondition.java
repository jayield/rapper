package com.github.jayield.rapper.utils;

public class EqualCondition<T> extends Condition<T> {

    public EqualCondition(String columnName, T value) {
        super(columnName, "=", value);
    }
}
