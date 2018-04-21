package org.github.isel.rapper.utils;

import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.ConnectionPoolDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.function.Supplier;

public class ConnectionManager {
    private static final Logger staticLogger = LoggerFactory.getLogger(ConnectionManager.class);
    private static ConnectionManager connectionManager = null;

    private final ConnectionPoolDataSource poolDataSource;

    private ConnectionManager(DBsPath envVarName){
        poolDataSource = getDataSource(envVarName);
    }

    public static ConnectionManager getConnectionManager(DBsPath envVar){
        if(connectionManager == null) {
            staticLogger.info("Creating new ConnectionManager for " +  envVar.name());
            connectionManager = new ConnectionManager(envVar);
        }
        return connectionManager;
    }

    private static ConnectionPoolDataSource getDataSource(DBsPath envVar){
        //CONNECTION STRING FORMAT: SERVERNAME;DATABASE;USER;PASSWORD
        String connectionString = System.getenv(envVar.toString());
        String[] connectionStringParts = connectionString.split(";");

        staticLogger.info("The connection string retrieved was " + connectionString + "\nTaken from " + envVar + " environment variable");

        SQLServerConnectionPoolDataSource dataSource = new SQLServerConnectionPoolDataSource();

        dataSource.setServerName(connectionStringParts[0]);
        dataSource.setDatabaseName(connectionStringParts[1]);
        dataSource.setUser(connectionStringParts[2]);
        dataSource.setPassword(connectionStringParts[3]);

        return dataSource;
    }

    public Connection getConnection() throws SQLException {
        Connection connection = poolDataSource.getPooledConnection().getConnection();
        connection.setAutoCommit(false);

        return connection;
    }
}
