package com.github.jayield.rapper;

import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.utils.MapperSettings;
import com.github.jayield.rapper.utils.SqlField;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class DomainObjectComparator<T extends DomainObject> implements Comparator<T> {

    private final SqlField.SqlFieldVersion versionField;
    private final List<SqlField> allFields;

    public DomainObjectComparator(MapperSettings mapperSettings) {
        versionField = mapperSettings.getVersionField();
        if(versionField != null) versionField.field.setAccessible(true);
        allFields = new ArrayList<>();
        allFields.addAll(mapperSettings.getIds());
        allFields.addAll(mapperSettings.getColumns());
    }

    //o1 should be the object in IdentityMap and o2 the new object to compare
    @Override
    public int compare(T o1, T o2) {
        boolean anyDifference = allFields.stream().anyMatch(sqlField -> {
            try {
                sqlField.field.setAccessible(true);
                Object o1Value = sqlField.field.get(o1);
                Object o2Value = sqlField.field.get(o2);
                if(o1Value == null && o2Value == null) return false;
                if (o1Value == null || o2Value == null) return true;
                return !o1Value.equals(o2Value);
            } catch (IllegalAccessException | IllegalArgumentException e) {
                try {
                    Object o1Value = sqlField.field.get(o1.getIdentityKey());
                    Object o2Value = sqlField.field.get(o2.getIdentityKey());
                    return !o1Value.equals(o2Value);
                } catch (IllegalAccessException e1) {
                    throw new DataMapperException(e1);
                }
            }
        });

        return anyDifference ? -1 : 0;
    }
}
