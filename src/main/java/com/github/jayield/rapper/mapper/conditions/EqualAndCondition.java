package com.github.jayield.rapper.mapper.conditions;

public class EqualAndCondition<T> extends Condition<T> {

    public EqualAndCondition(String columnName, T value) {
        super(columnName, "=", value);
    }
}
