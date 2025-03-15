package com.manu156.driver.redash;

import java.util.List;
import java.util.Map;

/**
 * Represents the results of a Redash query.
 */
public class RedashQueryResult {
    private final List<RedashColumn> columns;
    private final List<Map<String, Object>> rows;
    
    public RedashQueryResult(List<RedashColumn> columns, List<Map<String, Object>> rows) {
        this.columns = columns;
        this.rows = rows;
    }
    
    /**
     * Get the columns in the result.
     * 
     * @return List of columns
     */
    public List<RedashColumn> getColumns() {
        return columns;
    }
    
    /**
     * Get the rows in the result.
     * 
     * @return List of rows, where each row is a map of column name to value
     */
    public List<Map<String, Object>> getRows() {
        return rows;
    }
    
    /**
     * Get the number of rows in the result.
     * 
     * @return Number of rows
     */
    public int getRowCount() {
        return rows.size();
    }
    
    /**
     * Get the number of columns in the result.
     * 
     * @return Number of columns
     */
    public int getColumnCount() {
        return columns.size();
    }
    
    /**
     * Get a column by index.
     * 
     * @param index The column index (0-based)
     * @return The column
     */
    public RedashColumn getColumn(int index) {
        return columns.get(index);
    }
    
    /**
     * Get a column by name.
     * 
     * @param name The column name
     * @return The column, or null if not found
     */
    public RedashColumn getColumn(String name) {
        for (RedashColumn column : columns) {
            if (column.getName().equals(name)) {
                return column;
            }
        }
        return null;
    }
    
    /**
     * Get the index of a column by name.
     * 
     * @param name The column name
     * @return The column index (0-based), or -1 if not found
     */
    public int getColumnIndex(String name) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(name)) {
                return i;
            }
        }
        return -1;
    }
} 