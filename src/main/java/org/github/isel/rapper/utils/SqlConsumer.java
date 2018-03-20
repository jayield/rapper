package org.github.isel.rapper.utils;

import org.github.isel.rapper.exceptions.DataMapperException;

import java.sql.SQLException;
import java.util.function.Consumer;

public interface SqlConsumer<T> {
    void accept(T t) throws SQLException;

    default Consumer<T> wrap(){
        return t -> {
            try{ this.accept(t);}
            catch (SQLException e){
                throw new DataMapperException(e);
            }
        };
    }

}
