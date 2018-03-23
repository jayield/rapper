package org.github.isel.rapper.utils;

import org.github.isel.rapper.exceptions.DataMapperException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class SQLUtils {
    public static long getVersion(PreparedStatement statement) throws SQLException {
        long version;
        try (ResultSet inserted = statement.getResultSet()) {
            if (inserted.next()){
                version = inserted.getLong(1);
            }
            else throw new DataMapperException("Error inserting new entry");
        }
        return version;
    }

    public static long getGeneratedKey(PreparedStatement preparedStatement) throws SQLException {
        long jobId;
        try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
            if (generatedKeys.next()){
                jobId = generatedKeys.getLong(1);
            }
            else throw new DataMapperException("Error inserting new entry");
        }
        return jobId;
    }

    public static CompletableFuture<PreparedStatement> execute(String sqlQuery, Consumer<PreparedStatement> handleStatement){
        Connection con = UnitOfWork.getCurrent().getConnection();
        try{
            PreparedStatement preparedStatement = con.prepareStatement(sqlQuery);
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
}
