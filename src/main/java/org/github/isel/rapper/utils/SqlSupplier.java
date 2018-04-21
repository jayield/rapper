package org.github.isel.rapper.utils;

import org.github.isel.rapper.exceptions.DataMapperException;

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
