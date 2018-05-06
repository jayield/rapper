package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.*;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public class MapperRegistry {

    private MapperRegistry(){}

    private static Map<Class, Container> repositoryMap = new HashMap<>();

    public static<T extends DomainObject<K>, K> DataRepository<T, K> getRepository(Class<T> type) {
        Container container = repositoryMap.computeIfAbsent(type, aClass -> {
            MapperSettings mapperSettings = new MapperSettings(aClass);
            ExternalsHandler<T, K> externalHandler = new ExternalsHandler<>(mapperSettings);
            DataMapper<T, K> dataMapper = new DataMapper<T, K>(aClass, mapperSettings, externalHandler);

            Class<K> keyType = ReflectionUtils.<T, K>getKeyType(aClass);

            DataRepository<T, K> repository = new DataRepository<T, K>(aClass, keyType, dataMapper);

            return new Container<>(mapperSettings, externalHandler, repository, dataMapper);
        });

        return container.getDataRepository();
    }

    static <T extends DomainObject<K>, K> ExternalsHandler<T, K> getExternal(Class<T> type){
        return repositoryMap.get(type).getExternalsHandler();
    }

    public static MapperSettings getMapperSettings(Class<?> type){
        return repositoryMap.get(type).getMapperSettings();
    }

    public static <T extends DomainObject<K>, K> Mapper<T, K> getMapper(Class<T> type){
        return repositoryMap.get(type).getMapper();
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
