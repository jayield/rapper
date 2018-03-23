package org.github.isel.rapper.utils;

import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.ConnectionPoolDataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.github.isel.rapper.utils.ConnectionManager.DBsPath.TESTDB;

public class ConnectionManager {
    private final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);

    public enum DBsPath {
        DEFAULTDB ("DB_CONNECTION_STRING"),
        TESTDB ("DBTEST_CONNECTION_STRING");

        String value;
        DBsPath(String value) {
            this.value = value;
        }

        @Override
        public String toString(){
            return value;
        }
    }

    private static ConnectionManager connectionManager = null;

    public static ConnectionManager getConnectionManager(String envVar){
        if(connectionManager == null) connectionManager = new ConnectionManager(envVar);
        return connectionManager;
    }

    /**CONNECTION STRING FORMAT: SERVERNAME;DATABASE;USER;PASSWORD*/
    private final ConnectionPoolDataSource dataSource;

    public ConnectionManager(String envVarName){
        dataSource = getDataSource(envVarName);
    }

    public Connection getConnection() {
        try {
            Connection connection = dataSource
                    .getPooledConnection()
                    .getConnection();
            connection.setAutoCommit(false);
            return connection;
        } catch (SQLException e) {
            logger.info("Error on stablishing connection to the DB \n" + e.getMessage());
        }
        return null;
    }

    //test method
    public static Connection getConnection2() {
        try {
            Connection connection = getDataSource(TESTDB.toString())
                    .getPooledConnection()
                    .getConnection();
            connection.setAutoCommit(false);
            return connection;
        } catch (SQLException e) {
            Logger logger = LoggerFactory.getLogger(ConnectionManager.class);
            logger.info("Error on stablishing connection to the DB \n" + e.getMessage());
        }
        return null;
    }

    private static ConnectionPoolDataSource getDataSource(String envVar){
        String connectionString = System.getenv(envVar);
        String [] connectionStringParts = connectionString.split(";");

        SQLServerConnectionPoolDataSource dataSource = new SQLServerConnectionPoolDataSource();

        dataSource.setServerName(connectionStringParts[0]);
        dataSource.setDatabaseName(connectionStringParts[1]);
        dataSource.setUser(connectionStringParts[2]);
        dataSource.setPassword(connectionStringParts[3]);

        return dataSource;
    }
}
