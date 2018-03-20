package org.github.isel.rapper.utils;

import org.github.isel.rapper.exceptions.DataMapperException;

import java.sql.SQLException;
import java.util.function.Function;

@FunctionalInterface
public interface SqlFunction<T,R> {
    R apply(T t) throws SQLException;

    default Function<T,R> wrap(){
        return t -> {
            try {
                return this.apply(t);
            } catch (SQLException e) {
                throw new DataMapperException(e);
            }
        };
    }
}
