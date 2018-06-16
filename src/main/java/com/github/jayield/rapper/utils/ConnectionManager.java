package com.github.jayield.rapper.utils;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

public class ConnectionManager {
    private static final Logger staticLogger = LoggerFactory.getLogger(ConnectionManager.class);
    private static ConnectionManager connectionManager = null;

    private final JDBCClient client;
    private final String url;

    private ConnectionManager(String url, String user, String password){
        this.url = url;
        client = getDataSource(url, user, password);
    }

    /**
     * If there isn't a ConnectionManager instance it will create a new one connected to the given DBPath, else it will return the existing ConnectionManager
     * @param envVar
     * @return
     */
    public static ConnectionManager getConnectionManager(DBsPath envVar) {
        String[] connectionStringParts = new String[3];
        if (connectionManager == null)
            connectionStringParts = separateComponents(envVar);
        return getConnectionManager(
                connectionStringParts[0],
                connectionStringParts[1],
                connectionStringParts[2]);
    }

    public static ConnectionManager getConnectionManager(String url, String user, String password){
        if(connectionManager == null) {
            staticLogger.info("Creating new ConnectionManager for {};{};{}", url, user, password);
            connectionManager = new ConnectionManager(url, user, password);
        }
        return connectionManager;
    }

    public static ConnectionManager getConnectionManager(){
        return connectionManager;
    }

    private static String[] separateComponents(DBsPath envVar){
        //CONNECTION STRING FORMAT: URL%;USER%;PASSWORD
        String connectionString = System.getenv(envVar.toString());
        return connectionString.split("%;");
    }

    private static JDBCClient getDataSource(String url, String user, String password){
        return JDBCClient.createShared(Vertx.vertx(), new JsonObject()
            .put("url", url)
            .put("user", user)
            .put("password", password));
    }

    public CompletableFuture<SQLConnection> getConnection() {
        return SQLUtils.callbackToPromise(client::getConnection)
                .thenCompose(con -> SQLUtils.<Void>callbackToPromise(ar -> con.setAutoCommit(false, ar))
                        .thenApply(v -> con));
    }

    public String getUrl() {
        return url;
    }
}
