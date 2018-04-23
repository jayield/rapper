package org.github.isel.rapper.utils;

import org.github.isel.rapper.exceptions.DataMapperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class SQLUtils {
    private static final Logger logger = LoggerFactory.getLogger(SQLUtils.class);

    public static CompletableFuture<PreparedStatement> execute(String sqlQuery, Consumer<PreparedStatement> handleStatement){
        Connection con = UnitOfWork.getCurrent().getConnection();
        try{
            PreparedStatement preparedStatement = con.prepareStatement(sqlQuery, Statement.RETURN_GENERATED_KEYS);
            handleStatement.accept(preparedStatement);
            return CompletableFuture.supplyAsync(()-> {
                try {
                    preparedStatement.execute();
                    return preparedStatement;
                } catch (SQLException e) {
                    throw new DataMapperException(e);
                }
            });
        } catch (SQLException e) {
            throw new DataMapperException(e);
        }
    }

    public static void setValuesInStatement(Stream<? extends SqlField> fields, PreparedStatement stmt, Object obj){
        CollectionUtils.zipWithIndex(fields).forEach(entry -> entry.item.setValueInStatement(stmt, entry.index+1, obj));
    }
}
