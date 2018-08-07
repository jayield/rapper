package com.github.jayield.rapper.mapper.conditions;

public class EqualCondition<T> extends Condition<T> {

    public EqualCondition(String columnName, T value) {
        super(columnName, "=", value);
    }
}
