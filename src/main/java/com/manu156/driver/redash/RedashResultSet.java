package com.manu156.driver.redash;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * Implementation of java.sql.ResultSet for Redash API.
 * This is a forward-only, read-only result set.
 */
public class RedashResultSet implements ResultSet {
    
    private final RedashStatement statement;
    private final RedashQueryResult queryResult;
    private final List<RedashColumn> columns;
    private final List<Map<String, Object>> rows;
    private int currentRowIndex = -1;
    private boolean closed = false;
    private boolean wasNull = false;
    
    public RedashResultSet(RedashStatement statement, RedashQueryResult queryResult) {
        this.statement = statement;
        this.queryResult = queryResult;
        this.columns = queryResult.getColumns();
        this.rows = queryResult.getRows();
    }
    
    @Override
    public boolean next() throws SQLException {
        checkClosed();
        currentRowIndex++;
        return currentRowIndex < rows.size();
    }
    
    @Override
    public void close() throws SQLException {
        closed = true;
    }
    
    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        return wasNull;
    }
    
    @Override
    public String getString(int columnIndex) throws SQLException {
        Object value = getColumnValue(columnIndex);
        wasNull = value == null;
        return value == null ? null : value.toString();
    }
    
    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Object value = getColumnValue(columnIndex);
        wasNull = value == null;
        if (wasNull) return false;
        
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        } else if (value instanceof String) {
            String strValue = ((String) value).toLowerCase();
            return "true".equals(strValue) || "1".equals(strValue) || "yes".equals(strValue);
        }
        throw new SQLException("Cannot convert value to boolean: " + value);
    }
    
    @Override
    public byte getByte(int columnIndex) throws SQLException {
        Object value = getColumnValue(columnIndex);
        wasNull = value == null;
        if (wasNull) return 0;
        
        if (value instanceof Number) {
            return ((Number) value).byteValue();
        }
        throw new SQLException("Cannot convert value to byte: " + value);
    }
    
    @Override
    public short getShort(int columnIndex) throws SQLException {
        Object value = getColumnValue(columnIndex);
        wasNull = value == null;
        if (wasNull) return 0;
        
        if (value instanceof Number) {
            return ((Number) value).shortValue();
        }
        throw new SQLException("Cannot convert value to short: " + value);
    }
    
    @Override
    public int getInt(int columnIndex) throws SQLException {
        Object value = getColumnValue(columnIndex);
        wasNull = value == null;
        if (wasNull) return 0;
        
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        throw new SQLException("Cannot convert value to int: " + value);
    }
    
    @Override
    public long getLong(int columnIndex) throws SQLException {
        Object value = getColumnValue(columnIndex);
        wasNull = value == null;
        if (wasNull) return 0;
        
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        throw new SQLException("Cannot convert value to long: " + value);
    }
    
    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Object value = getColumnValue(columnIndex);
        wasNull = value == null;
        if (wasNull) return 0;
        
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        throw new SQLException("Cannot convert value to float: " + value);
    }
    
    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object value = getColumnValue(columnIndex);
        wasNull = value == null;
        if (wasNull) return 0;
        
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        throw new SQLException("Cannot convert value to double: " + value);
    }
    
    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        BigDecimal value = getBigDecimal(columnIndex);
        if (value != null) {
            try {
                return value.setScale(scale, BigDecimal.ROUND_HALF_UP);
            } catch (ArithmeticException e) {
                throw new SQLException("Error setting scale for BigDecimal", e);
            }
        }
        return null;
    }
    
    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        return value == null ? null : value.getBytes();
    }
    
    @Override
    public Date getDate(int columnIndex) throws SQLException {
        Object value = getColumnValue(columnIndex);
        wasNull = value == null;
        if (wasNull) return null;
        
        if (value instanceof Date) {
            return (Date) value;
        } else if (value instanceof java.util.Date) {
            return new Date(((java.util.Date) value).getTime());
        } else if (value instanceof String) {
            try {
                return Date.valueOf((String) value);
            } catch (IllegalArgumentException e) {
                throw new SQLException("Cannot convert value to Date: " + value);
            }
        }
        throw new SQLException("Cannot convert value to Date: " + value);
    }
    
    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Object value = getColumnValue(columnIndex);
        wasNull = value == null;
        if (wasNull) return null;
        
        if (value instanceof Time) {
            return (Time) value;
        } else if (value instanceof java.util.Date) {
            return new Time(((java.util.Date) value).getTime());
        } else if (value instanceof String) {
            try {
                return Time.valueOf((String) value);
            } catch (IllegalArgumentException e) {
                throw new SQLException("Cannot convert value to Time: " + value);
            }
        }
        throw new SQLException("Cannot convert value to Time: " + value);
    }
    
    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Object value = getColumnValue(columnIndex);
        wasNull = value == null;
        if (wasNull) return null;
        
        if (value instanceof Timestamp) {
            return (Timestamp) value;
        } else if (value instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) value).getTime());
        } else if (value instanceof String) {
            try {
                return Timestamp.valueOf((String) value);
            } catch (IllegalArgumentException e) {
                throw new SQLException("Cannot convert value to Timestamp: " + value);
            }
        }
        throw new SQLException("Cannot convert value to Timestamp: " + value);
    }
    
    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }
    
    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }
    
    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }
    
    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }
    
    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }
    
    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }
    
    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }
    
    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }
    
    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }
    
    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }
    
    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }
    
    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }
    
    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }
    
    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkClosed();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(columnLabel)) {
                return i + 1; // JDBC columns are 1-based
            }
        }
        throw new SQLException("Column not found: " + columnLabel);
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
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("Cursor names are not supported");
    }
    
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return new RedashResultSetMetaData(columns);
    }
    
    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getColumnValue(columnIndex);
    }
    
    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }
    
    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }
    
    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {

    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {

    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {

    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {

    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {

    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {

    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {

    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {

    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {

    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {

    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {

    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {

    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {

    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {

    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {

    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {

    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {

    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {

    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {

    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {

    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {

    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {

    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {

    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {

    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {

    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {

    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {

    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {

    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {

    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {

    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {

    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {

    }

    @Override
    public void insertRow() throws SQLException {

    }

    @Override
    public void updateRow() throws SQLException {

    }

    @Override
    public void deleteRow() throws SQLException {

    }

    @Override
    public void refreshRow() throws SQLException {

    }

    @Override
    public void cancelRowUpdates() throws SQLException {

    }

    @Override
    public void moveToInsertRow() throws SQLException {

    }

    @Override
    public void moveToCurrentRow() throws SQLException {

    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }
    
    // Helper methods
    
    private Object getColumnValue(int columnIndex) throws SQLException {
        checkClosed();
        if (currentRowIndex < 0) {
            throw new SQLException("No current row. Call next() first.");
        }
        if (currentRowIndex >= rows.size()) {
            throw new SQLException("No more rows available");
        }
        if (columnIndex < 1 || columnIndex > columns.size()) {
            throw new SQLException("Invalid column index: " + columnIndex);
        }
        
        Map<String, Object> currentRow = rows.get(currentRowIndex);
        String columnName = columns.get(columnIndex - 1).getName();
        return currentRow.get(columnName);
    }
    
    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet is closed");
        }
    }
    
    // Unsupported operations
    
    @Override
    public boolean isBeforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("Only forward iteration is supported");
    }
    
    @Override
    public boolean isAfterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("Only forward iteration is supported");
    }
    
    @Override
    public boolean isFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("Only forward iteration is supported");
    }
    
    @Override
    public boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("Only forward iteration is supported");
    }
    
    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("Only forward iteration is supported");
    }
    
    @Override
    public void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("Only forward iteration is supported");
    }
    
    @Override
    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException("Only forward iteration is supported");
    }
    
    @Override
    public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException("Only forward iteration is supported");
    }
    
    @Override
    public int getRow() throws SQLException {
        checkClosed();
        return currentRowIndex + 1;
    }
    
    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException("Only forward iteration is supported");
    }
    
    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("Only forward iteration is supported");
    }
    
    @Override
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException("Only forward iteration is supported");
    }
    
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLException("Only FETCH_FORWARD is supported");
        }
    }
    
    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }
    
    @Override
    public void setFetchSize(int rows) throws SQLException {
        // No-op
    }
    
    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }
    
    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Object value = getColumnValue(columnIndex);
        wasNull = value == null;
        if (wasNull) return null;
        
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        } else if (value instanceof Number) {
            return new BigDecimal(value.toString());
        } else if (value instanceof String) {
            try {
                return new BigDecimal((String) value);
            } catch (NumberFormatException e) {
                throw new SQLException("Cannot convert value to BigDecimal: " + value);
            }
        }
        throw new SQLException("Cannot convert value to BigDecimal: " + value);
    }
    
    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }
    
    // Additional unsupported operations
    
    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }
    
    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }
    
    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }
    
    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }
    
    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }
    
    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }
    
    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported");
    }
    
    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported");
    }
    
    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Custom type mapping is not supported");
    }
    
    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref type is not supported");
    }
    
    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob type is not supported");
    }
    
    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob type is not supported");
    }
    
    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array type is not supported");
    }
    
    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Custom type mapping is not supported");
    }
    
    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref type is not supported");
    }
    
    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob type is not supported");
    }
    
    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob type is not supported");
    }
    
    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array type is not supported");
    }
    
    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Calendar-based date conversion is not supported");
    }
    
    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Calendar-based date conversion is not supported");
    }
    
    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Calendar-based time conversion is not supported");
    }
    
    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Calendar-based time conversion is not supported");
    }
    
    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Calendar-based timestamp conversion is not supported");
    }
    
    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Calendar-based timestamp conversion is not supported");
    }
    
    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("URL type is not supported");
    }
    
    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("URL type is not supported");
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {

    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {

    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {

    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {

    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("Type-based object conversion is not supported");
    }
    
    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("Type-based object conversion is not supported");
    }
    
    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId is not supported");
    }
    
    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId is not supported");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {

    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {

    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob is not supported");
    }
    
    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob is not supported");
    }
    
    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML is not supported");
    }
    
    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML is not supported");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NString is not supported");
    }
    
    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("NString is not supported");
    }
    
    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacter streams are not supported");
    }
    
    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacter streams are not supported");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }
    
    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {

    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {

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