package org.github.isel.rapper.utils;

import org.github.isel.rapper.DataRepository;
import org.github.isel.rapper.DomainObject;

import java.util.HashMap;
import java.util.Map;

public class MapperRegistry {
    private static Map<Class, DataRepository> repositoryMap = new HashMap<>();

    public static<T extends DomainObject<K>, K> DataRepository<T, K> getRepository(Class<T> domainObject) {
        return repositoryMap.computeIfAbsent(domainObject, c -> new DataRepository<>(domainObject));
    }
}
