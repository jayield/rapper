package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.exceptions.DataMapperException;

import java.sql.SQLException;
import java.util.function.Supplier;

public interface SqlSupplier<T> {

    T get() throws SQLException;

    default Supplier<T> wrap(){
        return () -> {
            try{
                return get();
            }
            catch (Exception e){
                throw new DataMapperException(e);
            }
        };
    }
}
