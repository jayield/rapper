package com.github.jayield.rapper.utils;

import com.github.jayield.rapper.exceptions.DataMapperException;
import org.apache.commons.dbcp2.cpdsadapter.DriverAdapterCPDS;
import org.apache.commons.dbcp2.datasources.SharedPoolDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionManager {
    private static final Logger staticLogger = LoggerFactory.getLogger(ConnectionManager.class);
    private static ConnectionManager connectionManager = null;

    private final DataSource poolDataSource;

    private ConnectionManager(String url, String user, String password){
        poolDataSource = getDataSource( url, user, password);
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
            staticLogger.info("Creating new ConnectionManager for {}",url+";"+user+";"+password  );
            connectionManager = new ConnectionManager(url, user, password);
        }
        return connectionManager;
    }

    private static String[] separateComponents(DBsPath envVar){
        //CONNECTION STRING FORMAT: URL%;USER%;PASSWORD
        String connectionString = System.getenv(envVar.toString());
        return connectionString.split("%;");
    }

    private static DataSource getDataSource(String url, String user, String password){
        DriverAdapterCPDS cpds = new DriverAdapterCPDS();

        cpds.setUrl(url);
        cpds.setUser(user);
        cpds.setPassword(password);

        SharedPoolDataSource tds = new SharedPoolDataSource();
        tds.setConnectionPoolDataSource(cpds);
        return tds;
    }

    public Connection getConnection() throws SQLException {
        Connection connection = poolDataSource.getConnection();
        connection.setAutoCommit(false);

        return connection;
    }
}
