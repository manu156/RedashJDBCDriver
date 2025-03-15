package com.manu156.driver.redash;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of java.sql.PreparedStatement for Redash API.
 */
public class RedashPreparedStatement extends RedashStatement implements PreparedStatement {
    
    private final String sql;
    private final Map<Integer, Object> parameters = new HashMap<>();
    
    // Pattern to match "FROM query_123" or "FROM query_123 WHERE ..."
    private static final Pattern QUERY_ID_PATTERN = Pattern.compile(
            "FROM\\s+query_(\\d+)(?:\\s+|$)", Pattern.CASE_INSENSITIVE);
    
    public RedashPreparedStatement(RedashConnection connection, String sql) {
        super(connection);
        this.sql = sql;
    }
    
    @Override
    public ResultSet executeQuery() throws SQLException {
        checkClosed();
        
        try {
            // Extract query ID if present
            Matcher matcher = QUERY_ID_PATTERN.matcher(sql);
            if (matcher.find()) {
                String queryId = matcher.group(1);
                
                // Convert parameters to a map for the API client
                Map<String, Object> queryParams = new HashMap<>();
                for (Map.Entry<Integer, Object> entry : parameters.entrySet()) {
                    queryParams.put("p" + entry.getKey(), entry.getValue());
                }
                
                RedashQueryResult result = ((RedashConnection) getConnection()).getApiClient()
                        .executeQueryById(queryId, queryParams);
                
                RedashResultSet resultSet = new RedashResultSet(this, result);
                return resultSet;
            } else {
                // Get the first data source
                List<Map<String, String>> dataSources = ((RedashConnection) getConnection()).getApiClient().getDataSources();
                if (dataSources.isEmpty()) {
                    throw new SQLException("No data sources available in Redash");
                }
                String dataSourceId = dataSources.get(0).get("id");
                
                // Create a new query with parameters
                String queryId = ((RedashConnection) getConnection()).getApiClient().createQuery(
                    "JDBC Prepared Query " + System.currentTimeMillis(), // Unique name
                    "Prepared query created via JDBC driver",
                    dataSourceId,
                    sql
                );
                
                // Convert parameters to a map for the API client
                Map<String, Object> queryParams = new HashMap<>();
                for (Map.Entry<Integer, Object> entry : parameters.entrySet()) {
                    queryParams.put("p" + entry.getKey(), entry.getValue());
                }
                
                // Execute the query with parameters
                RedashQueryResult result = ((RedashConnection) getConnection()).getApiClient()
                        .executeQueryById(queryId, queryParams);
                
                RedashResultSet resultSet = new RedashResultSet(this, result);
                return resultSet;
            }
        } catch (Exception e) {
            throw new SQLException("Error executing prepared query: " + e.getMessage(), e);
        }
    }
    
    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLFeatureNotSupportedException("Update operations are not supported");
    }
    
    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, null);
    }
    
    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBytes is not supported");
    }
    
    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream is not supported");
    }
    
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setUnicodeStream is not supported");
    }
    
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream is not supported");
    }
    
    @Override
    public void clearParameters() throws SQLException {
        checkClosed();
        parameters.clear();
    }
    
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public boolean execute() throws SQLException {
        checkClosed();
        executeQuery();
        return true;
    }
    
    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch operations are not supported");
    }
    
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCharacterStream is not supported");
    }
    
    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setRef is not supported");
    }
    
    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBlob is not supported");
    }
    
    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob is not supported");
    }
    
    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setArray is not supported");
    }
    
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMetaData is not supported before executing the query");
    }
    
    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, null);
    }
    
    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, x.toString());
    }
    
    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("getParameterMetaData is not supported");
    }
    
    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setRowId is not supported");
    }
    
    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }
    
    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNCharacterStream is not supported");
    }
    
    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob is not supported");
    }
    
    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob is not supported");
    }
    
    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBlob is not supported");
    }
    
    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob is not supported");
    }
    
    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("setSQLXML is not supported");
    }
    
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }
    
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream is not supported");
    }
    
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream is not supported");
    }
    
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCharacterStream is not supported");
    }
    
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream is not supported");
    }
    
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream is not supported");
    }
    
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCharacterStream is not supported");
    }
    
    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNCharacterStream is not supported");
    }
    
    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob is not supported");
    }
    
    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBlob is not supported");
    }
    
    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob is not supported");
    }
} 