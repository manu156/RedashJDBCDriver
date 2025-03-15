package com.manu156.driver.redash;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * Implementation of java.sql.ResultSetMetaData for Redash API.
 * Provides metadata about the columns in a RedashResultSet.
 */
public class RedashResultSetMetaData implements ResultSetMetaData {
    
    private final List<RedashColumn> columns;
    
    public RedashResultSetMetaData(List<RedashColumn> columns) {
        this.columns = columns;
    }
    
    @Override
    public int getColumnCount() throws SQLException {
        return columns.size();
    }
    
    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        checkColumnIndex(column);
        return false; // Redash results are read-only
    }
    
    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        checkColumnIndex(column);
        return true; // Assume all string columns are case-sensitive
    }
    
    @Override
    public boolean isSearchable(int column) throws SQLException {
        checkColumnIndex(column);
        return true; // All columns can be used in WHERE clause
    }
    
    @Override
    public boolean isCurrency(int column) throws SQLException {
        checkColumnIndex(column);
        return false; // No specific currency type in Redash
    }
    
    @Override
    public int isNullable(int column) throws SQLException {
        checkColumnIndex(column);
        return ResultSetMetaData.columnNullable; // Assume all columns are nullable
    }
    
    @Override
    public boolean isSigned(int column) throws SQLException {
        checkColumnIndex(column);
        String type = columns.get(column - 1).getType().toLowerCase();
        return type.equals("integer") || type.equals("float");
    }
    
    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        checkColumnIndex(column);
        // Return reasonable defaults based on column type
        String type = columns.get(column - 1).getType().toLowerCase();
        switch (type) {
            case "integer":
                return 11; // -2147483648 to 2147483647
            case "float":
                return 24; // Scientific notation
            case "boolean":
                return 5; // "false" or "true"
            case "datetime":
                return 26; // "YYYY-MM-DD HH:mm:ss.SSSSSS"
            case "date":
                return 10; // "YYYY-MM-DD"
            case "string":
            default:
                return 50; // Default string length
        }
    }
    
    @Override
    public String getColumnLabel(int column) throws SQLException {
        checkColumnIndex(column);
        return columns.get(column - 1).getName();
    }
    
    @Override
    public String getColumnName(int column) throws SQLException {
        return getColumnLabel(column); // Use same name as label
    }
    
    @Override
    public String getSchemaName(int column) throws SQLException {
        checkColumnIndex(column);
        return ""; // No schema concept in Redash
    }
    
    @Override
    public int getPrecision(int column) throws SQLException {
        checkColumnIndex(column);
        String type = columns.get(column - 1).getType().toLowerCase();
        switch (type) {
            case "integer":
                return 10;
            case "float":
                return 15;
            default:
                return 0;
        }
    }
    
    @Override
    public int getScale(int column) throws SQLException {
        checkColumnIndex(column);
        String type = columns.get(column - 1).getType().toLowerCase();
        return type.equals("float") ? 6 : 0;
    }
    
    @Override
    public String getTableName(int column) throws SQLException {
        checkColumnIndex(column);
        return ""; // No direct table mapping in Redash results
    }
    
    @Override
    public String getCatalogName(int column) throws SQLException {
        checkColumnIndex(column);
        return ""; // No catalog concept in Redash
    }
    
    @Override
    public int getColumnType(int column) throws SQLException {
        checkColumnIndex(column);
        return columns.get(column - 1).getSqlType();
    }
    
    @Override
    public String getColumnTypeName(int column) throws SQLException {
        checkColumnIndex(column);
        return columns.get(column - 1).getType();
    }
    
    @Override
    public boolean isReadOnly(int column) throws SQLException {
        checkColumnIndex(column);
        return true; // All Redash results are read-only
    }
    
    @Override
    public boolean isWritable(int column) throws SQLException {
        checkColumnIndex(column);
        return false; // All Redash results are read-only
    }
    
    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        checkColumnIndex(column);
        return false; // All Redash results are read-only
    }
    
    @Override
    public String getColumnClassName(int column) throws SQLException {
        checkColumnIndex(column);
        return columns.get(column - 1).getJavaType().getName();
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
    
    // Helper method to validate column index
    private void checkColumnIndex(int column) throws SQLException {
        if (column < 1 || column > columns.size()) {
            throw new SQLException("Invalid column index: " + column);
        }
    }
} 