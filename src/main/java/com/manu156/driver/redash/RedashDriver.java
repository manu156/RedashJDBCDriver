package com.manu156.driver.redash;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC Driver implementation for Redash API.
 * This driver allows read-only access to Redash queries through the Redash API.
 */
public class RedashDriver implements Driver {
    
    private static final String URL_PREFIX = "jdbc:redash://";
    private static final int MAJOR_VERSION = 1;
    private static final int MINOR_VERSION = 0;
    
    static {
        try {
            DriverManager.registerDriver(new RedashDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register Redash JDBC driver", e);
        }
    }
    
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        
        return new RedashConnection(url, info);
    }
    
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith(URL_PREFIX);
    }
    
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        DriverPropertyInfo[] driverProps = new DriverPropertyInfo[1];
        
        DriverPropertyInfo apiKey = new DriverPropertyInfo("apiKey", info.getProperty("apiKey"));
        apiKey.description = "Redash API Key";
        apiKey.required = true;
        
        driverProps[0] = apiKey;
        
        return driverProps;
    }
    
    @Override
    public int getMajorVersion() {
        return MAJOR_VERSION;
    }
    
    @Override
    public int getMinorVersion() {
        return MINOR_VERSION;
    }
    
    @Override
    public boolean jdbcCompliant() {
        // This driver is not fully JDBC compliant as it only supports read operations
        return false;
    }
    
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("getParentLogger is not supported");
    }
} 