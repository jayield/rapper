package com.github.jayield.rapper.utils;

public class OrderCondition extends Condition<Void> {

    private OrderCondition(String columnName, String comparand) {
        super(columnName, comparand, null);
    }

    public static OrderCondition asc(String columnName) {
        return new OrderCondition(columnName, "ASC");
    }

    public static OrderCondition desc(String columnName) {
        return new OrderCondition(columnName, "DESC");
    }
}
