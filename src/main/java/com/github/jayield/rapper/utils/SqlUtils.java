package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.sql.SqlField;
import com.github.jayield.rapper.unitofwork.UnitOfWork;
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
import java.util.stream.Stream;

public class SqlUtils {

    private SqlUtils() {
    }

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

    public static JsonArray getValuesForStatement(Stream<? extends SqlField> fields, Object obj) {
        return fields.map(f-> f.getValuesForStatement(obj)).flatMap(objectStream -> objectStream).collect(CollectionUtils.toJsonArray());
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
