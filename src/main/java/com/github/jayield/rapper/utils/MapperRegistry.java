package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.DataRepository;
import com.github.jayield.rapper.DomainObject;
import com.github.jayield.rapper.DataMapper;

import java.util.HashMap;
import java.util.Map;

public class MapperRegistry {

    private MapperRegistry(){}

    private static Map<Class, DataRepository> repositoryMap = new HashMap<>();

    public static<T extends DomainObject<K>, K> DataRepository<T, K> getRepository(Class<T> domainObject) {
        return repositoryMap.computeIfAbsent(domainObject, c -> new DataRepository<>(new DataMapper<>(domainObject)));
    }
}
