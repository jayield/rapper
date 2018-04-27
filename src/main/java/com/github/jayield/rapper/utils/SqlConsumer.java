package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.exceptions.DataMapperException;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.function.Consumer;

public interface SqlConsumer<T> {
    void accept(T t) throws SQLException, NoSuchFieldException, IllegalAccessException, InvocationTargetException, InstantiationException;

    default Consumer<T> wrap(){
        return t -> {
            try{ this.accept(t); }
            catch (Exception e){//TODO
                throw new DataMapperException(e);
            }
        };
    }
}
