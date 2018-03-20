package org.github.isel.rapper.utils;

import org.github.isel.rapper.DataMapper;
import org.github.isel.rapper.DomainObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MapperRegistry {
    private static Map<Class, DataMapper> map = new HashMap<>(); //TODO load all entrys

    public static Optional<DataMapper> getMapper(Class domainObject) {
        return Optional.ofNullable(map.get(domainObject));
    }

    public static <T extends DomainObject<K>, K> void addEntry(Class<T> domainObjectClass, DataMapper<T, K> dataMapper) {
        map.put(domainObjectClass, dataMapper);
    }
}
