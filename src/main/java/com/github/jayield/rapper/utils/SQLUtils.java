package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.exceptions.DataMapperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class SQLUtils {

    private SQLUtils() {
    }

    private static final Logger logger = LoggerFactory.getLogger(SQLUtils.class);

    public static CompletableFuture<PreparedStatement> execute(String sqlQuery, Consumer<PreparedStatement> handleStatement) {
        UnitOfWork current = UnitOfWork.getCurrent();
        Connection con = current.getConnection();
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sqlQuery, Statement.RETURN_GENERATED_KEYS);
            handleStatement.accept(preparedStatement);
            logger.info("New Prepared Statement {} from Unit of Work {}", preparedStatement.hashCode(), current.hashCode());
            return CompletableFuture.supplyAsync(() -> {
                try {
                    preparedStatement.execute();
                    return preparedStatement;
                } catch (SQLException e) {
                    throw new DataMapperException(e);
                }
            });
        } catch (SQLException e) {
            return CompletableFuture.supplyAsync(() -> {
                throw new DataMapperException(e);
            });
        }
    }

    public static void setValuesInStatement(Stream<? extends SqlField> fields, PreparedStatement stmt, Object obj) {
        //offset is used when a single setValueInStatement sets multiple values (for example in setValueInStatement of SqlFieldExternal)
        int[] offset = {0};
        CollectionUtils.zipWithIndex(fields)
                .forEach(entry -> offset[0] = entry.item.setValueInStatement(stmt, entry.index + 1 + offset[0], obj));
    }
}
