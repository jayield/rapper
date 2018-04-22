package org.github.isel.rapper.utils;

import javafx.util.Pair;
import org.github.isel.rapper.exceptions.DataMapperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class SQLUtils {
    private static final Logger logger = LoggerFactory.getLogger(SQLUtils.class);

    public static long getVersion(PreparedStatement statement) throws SQLException {
        long version;
        try (ResultSet inserted = statement.getResultSet()) {
            if (inserted.next()){
                version = inserted.getLong(1);
            }
            else throw new DataMapperException("Couldn't get version.");
        }
        return version;
    }

    public static long getGeneratedKey(PreparedStatement preparedStatement) throws SQLException {
        logger.info("UpdateCount = " + preparedStatement.getUpdateCount());
        if(!preparedStatement.getMoreResults()) throw new DataMapperException("Couldn't get generated key.");

        long key;
        try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
            if (generatedKeys.next()){
                key = generatedKeys.getLong(1);
            }
            else throw new DataMapperException("Couldn't get generated key.");
        }
        return key;
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
