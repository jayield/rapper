package com.github.jayield.rapper;

import com.github.jayield.rapper.exceptions.DataMapperException;
import com.github.jayield.rapper.utils.MapperSettings;
import com.github.jayield.rapper.utils.SqlField;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class DomainObjectComparator<T> implements Comparator<T> {

    private final SqlField.SqlFieldVersion versionField;
    private final List<SqlField> allFields;

    public DomainObjectComparator(MapperSettings mapperSettings) {
        versionField = mapperSettings.getVersionField();
        allFields = new ArrayList<>();
        allFields.addAll(mapperSettings.getIds());
        allFields.addAll(mapperSettings.getColumns());
    }

    //o1 should be the object in IdentityMap and o2 the new object to compare
    @Override
    public int compare(T o1, T o2) {
        if(versionField != null){
            try {
                versionField.field.setAccessible(true);
                Object o1Version = versionField.field.get(o1);
                Object o2Version = versionField.field.get(o2);
                return o1Version.equals(o2Version) ? 0 : -1;
            } catch (IllegalAccessException e) {
                throw new DataMapperException(e);
            }
        }

        boolean anyDifference = allFields.stream().anyMatch(sqlField -> {
            try {
                Object o1Value = sqlField.field.get(o1);
                Object o2Value = sqlField.field.get(o2);
                return !o1Value.equals(o2Value);
            } catch (IllegalAccessException e) {
                throw new DataMapperException(e);
            }
        });

        return anyDifference ? -1 : 0;
    }
}
