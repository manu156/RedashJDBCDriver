package com.manu156.driver.redash;

/**
 * Represents a column in a Redash query result.
 */
public class RedashColumn {
    private final String name;
    private final String type;
    
    public RedashColumn(String name, String type) {
        this.name = name;
        this.type = type;
    }
    
    public String getName() {
        return name;
    }
    
    public String getType() {
        return type;
    }
    
    /**
     * Convert Redash column type to java.sql.Types.
     * 
     * @return The corresponding java.sql.Types value
     */
    public int getSqlType() {
        switch (type.toLowerCase()) {
            case "integer":
                return java.sql.Types.INTEGER;
            case "float":
                return java.sql.Types.DOUBLE;
            case "boolean":
                return java.sql.Types.BOOLEAN;
            case "datetime":
                return java.sql.Types.TIMESTAMP;
            case "date":
                return java.sql.Types.DATE;
            case "string":
            default:
                return java.sql.Types.VARCHAR;
        }
    }
    
    /**
     * Get the Java class that corresponds to this column type.
     * 
     * @return The corresponding Java class
     */
    public Class<?> getJavaType() {
        switch (type.toLowerCase()) {
            case "integer":
                return Integer.class;
            case "float":
                return Double.class;
            case "boolean":
                return Boolean.class;
            case "datetime":
                return java.sql.Timestamp.class;
            case "date":
                return java.sql.Date.class;
            case "string":
            default:
                return String.class;
        }
    }
} 