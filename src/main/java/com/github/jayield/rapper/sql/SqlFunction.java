package com.github.jayield.rapper.sql;

import com.github.jayield.rapper.exceptions.DataMapperException;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Function;

public interface SqlFunction<T,R> {
    R apply(T t) throws IllegalAccessException, InvocationTargetException, InstantiationException;

    default Function<T,R> wrap(){
        return t -> {
            try{ return this.apply(t); }
            catch (Exception e){
                throw new DataMapperException(e);
            }
        };
    }
}
