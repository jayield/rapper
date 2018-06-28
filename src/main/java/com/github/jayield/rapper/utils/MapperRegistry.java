package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.*;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class MapperRegistry {

    private MapperRegistry(){}

    private static Map<Class, Container> repositoryMap = new HashMap<>();

    public static<T extends DomainObject<K>, K> Container<T, K> getContainer(Class<T> type) {
        return repositoryMap.computeIfAbsent(type, aClass -> {
            MapperSettings mapperSettings = new MapperSettings(aClass);
            ExternalsHandler<T, K> externalHandler = new ExternalsHandler<>(mapperSettings);
            DataMapper<T, K> dataMapper = new DataMapper<>(aClass, mapperSettings);

            Comparator<T> comparator = new DomainObjectComparator<>(mapperSettings);
            Class<K> keyType = ReflectionUtils.<T, K>getKeyType(aClass);

            DataRepository<T, K> repository = new DataRepository<>(aClass, keyType, dataMapper, externalHandler, comparator);

            return new Container<>(mapperSettings, externalHandler, repository, dataMapper);
        });
    }

    public static<T extends DomainObject<K>, K> DataRepository<T, K> getRepository(Class<T> type) {
        return getContainer(type).getDataRepository();
    }

    public static <T extends DomainObject<K>, K> ExternalsHandler<T, K> getExternal(Class<T> type){
        return getContainer(type).getExternalsHandler();
    }

    public static <T extends DomainObject<K>, K> MapperSettings getMapperSettings(Class<T> type){
        return getContainer(type).getMapperSettings();
    }

    public static <T extends DomainObject<K>, K> Mapper<T, K> getMapper(Class<T> type){
        return getContainer(type).getMapper();
    }

    public static class Container<T extends DomainObject<K>, K>{
        private final MapperSettings mapperSettings;
        private final ExternalsHandler<T, K> externalsHandler;
        private final DataRepository<T, K> dataRepository;
        private final Mapper<T, K> dataMapper;

        public Container(MapperSettings mapperSettings, ExternalsHandler<T, K> externalsHandler, DataRepository<T, K> dataRepository, Mapper<T, K> dataMapper){
            this.mapperSettings = mapperSettings;
            this.externalsHandler = externalsHandler;
            this.dataRepository = dataRepository;
            this.dataMapper = dataMapper;
        }

        MapperSettings getMapperSettings() {
            return mapperSettings;
        }

        ExternalsHandler<T, K> getExternalsHandler() {
            return externalsHandler;
        }

        DataRepository<T, K> getDataRepository() {
            return dataRepository;
        }

        Mapper<T, K> getMapper() {
            return dataMapper;
        }
    }
}
