package org.github.isel.rapper.utils;

import org.github.isel.rapper.exceptions.DataMapperException;

import java.sql.SQLException;
import java.util.function.Consumer;

public interface SqlConsumer<T> {
    void accept(T t) throws SQLException, NoSuchFieldException, IllegalAccessException;

    default Consumer<T> wrap(){
        return t -> {
            try{ this.accept(t);}
            catch (Exception e){//TODO
                throw new DataMapperException(e);
            }
        };
    }
}
