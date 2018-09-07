package com.github.jayield.rapper.mapper.conditions;

public class Condition<T> {
    private final String columnName;
    private final String comparand;
    private final T value;

    public Condition(String columnName, String comparand, T value) {
        this.columnName = columnName;
        this.comparand = comparand;
        this.value = value;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getComparand() {
        return comparand;
    }

    public T getValue() {
        return value;
    }
}
