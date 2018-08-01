package com.github.jayield.rapper.mapper;

import com.github.jayield.rapper.*;
import com.github.jayield.rapper.mapper.externals.ExternalsHandler;
import com.github.jayield.rapper.unitofwork.UnitOfWork;
import com.github.jayield.rapper.utils.DomainObjectComparator;

import java.util.HashMap;
import java.util.Map;

public class MapperRegistry {

    private MapperRegistry(){}

    private static Map<Class, Container> containerMap = new HashMap<>();

    public static<T extends DomainObject<K>, K> Container<T, K> getContainer(Class<T> type) {
        return containerMap.computeIfAbsent(type, aClass -> {
            MapperSettings mapperSettings = new MapperSettings(aClass);
            ExternalsHandler<T, K> externalsHandler = new ExternalsHandler<>(mapperSettings);

            return new Container<>(mapperSettings, externalsHandler);
        });
    }

    public static<T extends DomainObject<K>, K> DataRepository<T, K> getRepository(Class<T> type, UnitOfWork unit) {
        Container<T,K> container = getContainer(type);
        return new DataRepository<>(
                type,
                new DataMapper<>(type, container.getExternalsHandler(), container.getMapperSettings(), unit),
                new DomainObjectComparator<>(container.getMapperSettings()),
                unit);
    }

    public static <T extends DomainObject<K>, K> ExternalsHandler<T, K> getExternal(Class<T> type){
        return getContainer(type).getExternalsHandler();
    }

    public static <T extends DomainObject<K>, K> MapperSettings getMapperSettings(Class<T> type){
        return getContainer(type).getMapperSettings();
    }

//    public static <T extends DomainObject<K>, K> Mapper<T, K> getMapper(Class<T> type){
//        return getContainer(type).getMapper();
//    }

    public static class Container<T extends DomainObject<K>, K>{
        private final MapperSettings mapperSettings;
        private final ExternalsHandler<T, K> externalsHandler;

        public Container(MapperSettings mapperSettings, ExternalsHandler<T, K> externalsHandler){
            this.mapperSettings = mapperSettings;
            this.externalsHandler = externalsHandler;
        }

        public MapperSettings getMapperSettings() {
            return mapperSettings;
        }

        ExternalsHandler<T, K> getExternalsHandler() {
            return externalsHandler;
        }
    }
}
