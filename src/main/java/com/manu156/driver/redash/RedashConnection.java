package com.manu156.driver.redash;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.logging.Logger;

/**
 * Implementation of java.sql.Connection for Redash API.
 */
public class RedashConnection implements Connection {
    
    private final String host;
    private final int port;
    private final String apiKey;
    private boolean closed = false;
    private boolean autoCommit = true;
    private int transactionIsolation = Connection.TRANSACTION_NONE;
    private final RedashApiClient apiClient;
    
    private static final Pattern URL_PATTERN = Pattern.compile(
            "jdbc:redash://([^:]+)(?::(\\d+))?(?:/)?(?:\\?(.*))?");
    
    private static final Logger logger = Logger.getLogger(RedashConnection.class.getName());
    
    public RedashConnection(String url, Properties info) throws SQLException {
        Matcher matcher = URL_PATTERN.matcher(url);
        if (!matcher.matches()) {
            throw new SQLException("Invalid Redash JDBC URL: " + url);
        }
        
        this.host = matcher.group(1);
        String portStr = matcher.group(2);
        this.port = (portStr != null) ? Integer.parseInt(portStr) : 80;
        
        // Extract API key from URL parameters or properties
        String urlParams = matcher.group(3);
        String apiKeyFromUrl = null;
        if (urlParams != null) {
            String[] params = urlParams.split("&");
            for (String param : params) {
                String[] keyValue = param.split("=");
                if (keyValue.length == 2 && "apiKey".equals(keyValue[0])) {
                    apiKeyFromUrl = keyValue[1];
                    break;
                }
            }
        }
        
        this.apiKey = apiKeyFromUrl != null ? apiKeyFromUrl : info.getProperty("apiKey");
        if (this.apiKey == null || this.apiKey.isEmpty()) {
            throw new SQLException("API key is required for Redash JDBC connection");
        }
        
        this.apiClient = new RedashApiClient(host, port, apiKey);
    }
    
    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return new RedashStatement(this);
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return new RedashPreparedStatement(this, sql);
    }
    
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Callable statements are not supported");
    }
    
    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkClosed();
        return sql;
    }
    
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        this.autoCommit = autoCommit;
    }
    
    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return autoCommit;
    }
    
    @Override
    public void commit() throws SQLException {
        checkClosed();
        // No-op for read-only connection
    }
    
    @Override
    public void rollback() throws SQLException {
        checkClosed();
        // No-op for read-only connection
    }
    
    @Override
    public void close() throws SQLException {
        if (!closed) {
            closed = true;
            // Clean up resources if needed
        }
    }
    
    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }
    
    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new RedashDatabaseMetaData(this);
    }
    
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
        if (!readOnly) {
            // ignore the request to set the connection to read-write
            // throw new SQLException("Redash connection is read-only");
        }
    }
    
    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return true;
    }
    
    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkClosed();
        // No-op, catalogs not supported
    }
    
    @Override
    public String getCatalog() throws SQLException {
        checkClosed();
        return null;
    }
    
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        if (level != Connection.TRANSACTION_NONE) {
            throw new SQLException("Only TRANSACTION_NONE isolation level is supported");
        }
        this.transactionIsolation = level;
    }
    
    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return transactionIsolation;
    }
    
    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }
    
    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
    }
    
    // Helper method to check if connection is closed
    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed");
        }
    }
    
    // Get the API client
    RedashApiClient getApiClient() {
        return apiClient;
    }
    
    // Unsupported methods
    
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("Only TYPE_FORWARD_ONLY and CONCUR_READ_ONLY are supported");
        }
        return createStatement();
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("Only TYPE_FORWARD_ONLY and CONCUR_READ_ONLY are supported");
        }
        return prepareStatement(sql);
    }
    
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("Callable statements are not supported");
    }
    
    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("getTypeMap is not supported");
    }
    
    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("setTypeMap is not supported");
    }
    
    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException("setHoldability is not supported");
    }
    
    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }
    
    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }
    
    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }
    
    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }
    
    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }
    
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("createStatement with holdability is not supported");
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement with holdability is not supported");
    }
    
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Callable statements are not supported");
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("Auto-generated keys are not supported");
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Auto-generated keys are not supported");
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Auto-generated keys are not supported");
    }
    
    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createClob is not supported");
    }
    
    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createBlob is not supported");
    }
    
    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createNClob is not supported");
    }
    
    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("createSQLXML is not supported");
    }
    
    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw new SQLException("Timeout value cannot be negative");
        }
        
        if (closed) {
            return false;
        }
        
        try {
            // Test the connection using the API client
            String isConnected = apiClient.testConnection(timeout);
            
            if (isConnected != null) {
                logger.warning("Connection validation failed for Redash server at " + host + ":" + port + "error: " + isConnected);
            }
            
            return true;
        } catch (Exception e) {
            logger.severe("Error validating Redash connection: " + e.getMessage());
            return false;
        }
    }
    
    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        // No-op, client info not supported
    }
    
    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        // No-op, client info not supported
    }
    
    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }
    
    @Override
    public Properties getClientInfo() throws SQLException {
        return new Properties();
    }
    
    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("createArrayOf is not supported");
    }
    
    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("createStruct is not supported");
    }
    
    @Override
    public void setSchema(String schema) throws SQLException {
        // No-op, schemas not supported
    }
    
    @Override
    public String getSchema() throws SQLException {
        return null;
    }
    
    @Override
    public void abort(Executor executor) throws SQLException {
        if (executor == null) {
            throw new SQLException("Executor cannot be null");
        }
        executor.execute(() -> {
            try {
                close();
            } catch (SQLException e) {
                // Ignore
            }
        });
    }
    
    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNetworkTimeout is not supported");
    }
    
    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException("getNetworkTimeout is not supported");
    }
    
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }
    
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }
} 