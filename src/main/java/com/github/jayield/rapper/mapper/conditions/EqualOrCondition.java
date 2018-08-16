package com.github.jayield.rapper.mapper.conditions;

public class EqualOrCondition<T> extends Condition<T> {

    public EqualOrCondition(String columnName, T value) {
        super(columnName, "=", value);
    }
}
