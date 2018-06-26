package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.exceptions.DataMapperException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
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

    public static CompletableFuture<ResultSet> query(String sql, JsonArray params){
        UnitOfWork current = UnitOfWork.getCurrent();
        CompletableFuture<SQLConnection> con = current.getConnection();
        return con.thenCompose(connection -> callbackToPromise(ar -> connection.queryWithParams(sql, params, ar)));
    }

    public static CompletableFuture<ResultSet> queryAsyncParams(String sql, CompletableFuture<JsonArray> params){
        UnitOfWork curr = UnitOfWork.getCurrent();
        return params.thenCompose(jsonArray -> UnitOfWork.executeActionWithinUnit(curr, () -> query(sql, jsonArray)));
    }

    public static CompletableFuture<UpdateResult> update(String sql, JsonArray params){
        UnitOfWork current = UnitOfWork.getCurrent();
        CompletableFuture<SQLConnection> con = current.getConnection();
        return con.thenCompose(connection -> callbackToPromise(ar -> connection.updateWithParams(sql, params, ar)));
    }

    public static CompletableFuture<UpdateResult> updateAsyncParams(String sql, CompletableFuture<JsonArray> params){
        UnitOfWork curr = UnitOfWork.getCurrent();
        return params.thenCompose(jsonArray -> UnitOfWork.executeActionWithinUnit(curr, () -> update(sql, jsonArray)));
    }

    public static CompletableFuture<JsonArray> setValuesInStatement(Stream<? extends SqlField> fields, Object obj) {
        //offset is used when a single setValueInStatement sets multiple values (for example in setValueInStatement of SqlFieldExternal)
//        int[] offset = {0};
//        CollectionUtils.zipWithIndex(fields)
//                .forEach(entry -> offset[0] = entry.item.setValueInStatement(stmt, entry.index + 1 + offset[0], obj));
//        JsonArray jsonArray = new JsonArray();

        return CollectionUtils.listToCompletableFuture(fields.map(f-> f.setValueInStatement(obj)).collect(Collectors.toList()))
                .thenApply(list -> list.stream().flatMap(s-> s).collect(CollectionUtils.toJsonArray()));
//        fields.forEach(f -> f.setValueInStatement(obj));
//        return jsonArray;
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
