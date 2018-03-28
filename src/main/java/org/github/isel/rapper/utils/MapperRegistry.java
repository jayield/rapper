package org.github.isel.rapper.utils;

import org.github.isel.rapper.DataMapper;
import org.github.isel.rapper.DomainObject;

import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.lang.System.out;

public class MapperRegistry {
    private static Map<Class, DataMapper> map = new HashMap<>(); //TODO load all entrys

    public static<T extends DomainObject> DataMapper getMapper(Class<T> domainObject) {
        DataMapper mapper = map.computeIfAbsent(domainObject, c -> new DataMapper<>(domainObject));
        mapper.getMapperSettings()
                .getExternals().stream()
                .map(f -> f.type)
                .filter(c -> !map.containsKey(c))
                .filter(DomainObject.class::isAssignableFrom)
                .forEach(MapperRegistry::getMapper);
        return mapper;
    }

    public static <T extends DomainObject<K>, K> void addEntry(Class<T> domainObjectClass, DataMapper<T, K> dataMapper) {
        map.put(domainObjectClass, dataMapper);
    }
}
