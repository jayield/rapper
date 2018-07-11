package com.github.jayield.rapper.utils;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SQLUtils {

    private SQLUtils() {
    }

    private static final Logger logger = LoggerFactory.getLogger(SQLUtils.class);

    public static CompletableFuture<ResultSet> query(String sql, UnitOfWork unit, JsonArray params){
        CompletableFuture<SQLConnection> con = unit.getConnection();
        return con.thenCompose(connection -> callbackToPromise(ar -> connection.queryWithParams(sql, params, ar)));
    }

    public static CompletableFuture<ResultSet> queryAsyncParams(String sql, UnitOfWork unit, CompletableFuture<JsonArray> params){
        return params.thenCompose(jsonArray -> query(sql, unit, jsonArray));
    }

    public static CompletableFuture<UpdateResult> update(String sql, UnitOfWork unit, JsonArray params){
        CompletableFuture<SQLConnection> con = unit.getConnection();
        return con.thenCompose(connection -> callbackToPromise(ar -> connection.updateWithParams(sql, params, ar)));
    }

    public static CompletableFuture<UpdateResult> updateAsyncParams(String sql, UnitOfWork unit, CompletableFuture<JsonArray> params){
        return params.thenCompose(jsonArray -> update(sql, unit, jsonArray));
    }

    public static CompletableFuture<JsonArray> setValuesInStatement(Stream<? extends SqlField> fields, Object obj) {
        return CollectionUtils
                .listToCompletableFuture(fields.map(f-> f.setValueInStatement(obj)).collect(Collectors.toList()))
                .thenApply(list -> list.stream().flatMap(s-> s).collect(CollectionUtils.toJsonArray()));
    }

    public static <T> CompletableFuture<T> callbackToPromise(Consumer<Handler<AsyncResult<T>>> handler){
        CompletableFuture<T> future = new CompletableFuture<>();

        handler.accept(res -> {
            if(res.failed())
                future.completeExceptionally(res.cause());
            else
                future.complete(res.result());
        });

        return future;
    }
}
