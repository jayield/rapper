package org.github.isel.rapper.utils;

import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.ConnectionPoolDataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionManager {
    private static final Logger staticLogger = LoggerFactory.getLogger(ConnectionManager.class);
    private static ConnectionManager connectionManager = null;

    private final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);
    private final ConnectionPoolDataSource dataSource; /**CONNECTION STRING FORMAT: SERVERNAME;DATABASE;USER;PASSWORD*/

    private ConnectionManager(String envVarName){
        dataSource = getDataSource(envVarName);
    }

    public static ConnectionManager getConnectionManager(DBsPath envVar){
        if(connectionManager == null) connectionManager = new ConnectionManager(envVar.toString());
        return connectionManager;
    }

    private static ConnectionPoolDataSource getDataSource(String envVar){
        String connectionString = System.getenv(envVar);
        staticLogger.info(connectionString +" - "+ envVar);
        String [] connectionStringParts = connectionString.split(";");

        SQLServerConnectionPoolDataSource dataSource = new SQLServerConnectionPoolDataSource();

        dataSource.setServerName(connectionStringParts[0]);
        dataSource.setDatabaseName(connectionStringParts[1]);
        dataSource.setUser(connectionStringParts[2]);
        dataSource.setPassword(connectionStringParts[3]);

        return dataSource;
    }

    public Connection getConnection() {
        try {
            Connection connection = dataSource
                    .getPooledConnection()
                    .getConnection();
            connection.setAutoCommit(false);
            return connection;
        } catch (SQLException e) {
            logger.info("Error on establishing connection to the DB \n" + e.getMessage());
        }
        return null;
    }
}
