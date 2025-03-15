package com.manu156.driver.redash;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of java.sql.Statement for Redash API.
 */
public class RedashStatement implements Statement {
    
    private final RedashConnection connection;
    private RedashResultSet currentResultSet;
    private int updateCount = -1;
    private int maxRows = 0;
    private int queryTimeout = 0;
    private boolean closed = false;
    private int fetchSize = 0;
    private int fetchDirection = ResultSet.FETCH_FORWARD;
    
    // Pattern to match "FROM query_123" or "FROM query_123 WHERE ..."
    private static final Pattern QUERY_ID_PATTERN = Pattern.compile(
            "FROM\\s+query_(\\d+)(?:\\s+|$)", Pattern.CASE_INSENSITIVE);
    
    public RedashStatement(RedashConnection connection) {
        this.connection = connection;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        
        String trimmedSql = sql.trim().toUpperCase();
        if (!trimmedSql.equals("SHOW DATABASES") &&
            !trimmedSql.equals("SHOW TABLES") &&
            !trimmedSql.startsWith("EXPLAIN") && 
            !trimmedSql.startsWith("SELECT")) {
            throw new SQLException("Only SHOW DATABASES, SHOW TABLES, EXPLAIN, and SELECT are supported");
        }
        
        executeQuery(sql);
        return true;
    }
    
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        
        try {
            String trimmedSql = sql.trim().toUpperCase();
            
            // Handle SHOW DATABASES command
            if (trimmedSql.equals("SHOW DATABASES")) {
                List<Map<String, String>> dataSources = connection.getApiClient().getDataSources();
                if (dataSources.isEmpty()) {
                    throw new SQLException("No data sources available in Redash");
                }
                
                // Create columns and rows for the result set
                List<RedashColumn> columns = List.of(new RedashColumn("Database", "string"));
                List<Map<String, Object>> rows = dataSources.stream()
                    .map(ds -> {
                        Map<String, Object> row = new HashMap<>();
                        row.put("Database", ds.get("name"));
                        return row;
                    })
                    .toList();
                
                currentResultSet = new RedashResultSet(this, new RedashQueryResult(columns, rows));
                updateCount = -1;
                return currentResultSet;
            }
            
            // Handle SHOW TABLES command
            if (trimmedSql.equals("SHOW TABLES")) {
                List<Map<String, String>> queries = connection.getApiClient().getQueries();
                if (queries.isEmpty()) {
                    throw new SQLException("No queries available in Redash");
                }
                
                // Create columns and rows for the result set
                List<RedashColumn> columns = List.of(new RedashColumn("Tables_in_redash", "string"));
                List<Map<String, Object>> rows = queries.stream()
                    .map(q -> {
                        Map<String, Object> row = new HashMap<>();
                        row.put("Tables_in_redash", q.get("name"));
                        return row;
                    })
                    .toList();
                
                currentResultSet = new RedashResultSet(this, new RedashQueryResult(columns, rows));
                updateCount = -1;
                return currentResultSet;
            }
            
            // Handle EXPLAIN command
            if (trimmedSql.startsWith("EXPLAIN")) {
                String actualQuery = sql.substring(7).trim();
                // Get the first data source
                List<Map<String, String>> dataSources = connection.getApiClient().getDataSources();
                if (dataSources.isEmpty()) {
                    throw new SQLException("No data sources available in Redash");
                }
                String dataSourceId = dataSources.get(0).get("id");
                
                // Create a new query with EXPLAIN
                String queryId = connection.getApiClient().createQuery(
                    "EXPLAIN Query " + System.currentTimeMillis(),
                    "Query created via JDBC driver EXPLAIN",
                    dataSourceId,
                    "EXPLAIN " + actualQuery
                );
                
                // Execute the EXPLAIN query
                RedashQueryResult result = connection.getApiClient().executeQueryById(queryId, new HashMap<>());
                currentResultSet = new RedashResultSet(this, result);
                updateCount = -1;
                return currentResultSet;
            }
            
            // Extract query ID if present
            Matcher matcher = QUERY_ID_PATTERN.matcher(sql);
            if (matcher.find()) {
                String queryId = matcher.group(1);
                RedashQueryResult result = connection.getApiClient().executeQueryById(queryId, new HashMap<>());
                currentResultSet = new RedashResultSet(this, result);
                updateCount = -1;
                return currentResultSet;
            } else {
                // Get the first data source
                List<Map<String, String>> dataSources = connection.getApiClient().getDataSources();
                if (dataSources.isEmpty()) {
                    throw new SQLException("No data sources available in Redash");
                }
                String dataSourceId = dataSources.get(0).get("id");
                
                // Create a new query
                String queryId = connection.getApiClient().createQuery(
                    "JDBC Query " + System.currentTimeMillis(), // Unique name
                    "Query created via JDBC driver",
                    dataSourceId,
                    sql
                );
                
                // Execute the query
                RedashQueryResult result = connection.getApiClient().executeQueryById(queryId, new HashMap<>());
                currentResultSet = new RedashResultSet(this, result);
                updateCount = -1;
                return currentResultSet;
            }
        } catch (Exception e) {
            throw new SQLException("Error executing query: " + e.getMessage(), e);
        }
    }
    
    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Update operations are not supported");
    }
    
    @Override
    public void close() throws SQLException {
        if (!closed) {
            if (currentResultSet != null && !currentResultSet.isClosed()) {
                currentResultSet.close();
            }
            currentResultSet = null;
            closed = true;
        }
    }
    
    @Override
    public int getMaxFieldSize() throws SQLException {
        checkClosed();
        return 0;
    }
    
    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        checkClosed();
        // No-op
    }
    
    @Override
    public int getMaxRows() throws SQLException {
        checkClosed();
        return maxRows;
    }
    
    @Override
    public void setMaxRows(int max) throws SQLException {
        checkClosed();
        if (max < 0) {
            throw new SQLException("Max rows cannot be negative");
        }
        this.maxRows = max;
    }
    
    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkClosed();
        // No-op
    }
    
    @Override
    public int getQueryTimeout() throws SQLException {
        checkClosed();
        return queryTimeout;
    }
    
    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkClosed();
        if (seconds < 0) {
            throw new SQLException("Query timeout cannot be negative");
        }
        this.queryTimeout = seconds;
    }
    
    @Override
    public void cancel() throws SQLException {
        checkClosed();
        // Not supported
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
    
    @Override
    public void setCursorName(String name) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Named cursors are not supported");
    }
    
    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        return currentResultSet;
    }
    
    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        return updateCount;
    }
    
    @Override
    public boolean getMoreResults() throws SQLException {
        checkClosed();
        if (currentResultSet != null) {
            currentResultSet.close();
            currentResultSet = null;
        }
        updateCount = -1;
        return false;
    }
    
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLException("Only FETCH_FORWARD is supported");
        }
        this.fetchDirection = direction;
    }
    
    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return fetchDirection;
    }
    
    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        if (rows < 0) {
            throw new SQLException("Fetch size cannot be negative");
        }
        this.fetchSize = rows;
    }
    
    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return fetchSize;
    }
    
    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }
    
    @Override
    public int getResultSetType() throws SQLException {
        checkClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }
    
    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch operations are not supported");
    }
    
    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch operations are not supported");
    }
    
    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch operations are not supported");
    }
    
    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return connection;
    }
    
    @Override
    public boolean getMoreResults(int current) throws SQLException {
        checkClosed();
        if (current != CLOSE_CURRENT_RESULT) {
            throw new SQLException("Only CLOSE_CURRENT_RESULT is supported");
        }
        return getMoreResults();
    }
    
    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("Generated keys are not supported");
    }
    
    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("Update operations are not supported");
    }
    
    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Update operations are not supported");
    }
    
    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Update operations are not supported");
    }
    
    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("Auto-generated keys are not supported");
    }
    
    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Auto-generated keys are not supported");
    }
    
    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Auto-generated keys are not supported");
    }
    
    @Override
    public int getResultSetHoldability() throws SQLException {
        checkClosed();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }
    
    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }
    
    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkClosed();
        // No-op
    }
    
    @Override
    public boolean isPoolable() throws SQLException {
        checkClosed();
        return false;
    }
    
    @Override
    public void closeOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException("closeOnCompletion is not supported");
    }
    
    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
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
    
    // Helper method to check if statement is closed
    public void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed");
        }
    }
} 