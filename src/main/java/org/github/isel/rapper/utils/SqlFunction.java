package org.github.isel.rapper.utils;

import org.github.isel.rapper.exceptions.DataMapperException;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Function;

@FunctionalInterface
public interface SqlFunction<T,R> {
    R apply(T t) throws SQLException, NoSuchFieldException, IllegalAccessException, InvocationTargetException, InstantiationException;

    default Function<T,R> wrap(){
        return t -> {
            try {
                return this.apply(t);
            } catch (Exception e) {
                throw new DataMapperException(e);
            }
        };
    }

    default <V> SqlFunction<V, R> compose(SqlFunction<? super V, ? extends T> before) {
        Objects.requireNonNull(before);
        return (V v) -> apply(before.apply(v));
    }
}
