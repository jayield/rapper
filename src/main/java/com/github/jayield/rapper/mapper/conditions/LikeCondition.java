package com.github.jayield.rapper.mapper.conditions;

public class LikeCondition extends Condition<String> {

    public LikeCondition(String columnName, String value) {
        super(columnName, "LIKE", value != null ? String.format("%%%s%%", value) : null);
    }
}
