# Redash JDBC Driver

A Java SQL Driver implementation that connects to Redash through its API for read-only operations.

## Overview

This JDBC driver allows Java applications to connect to Redash as if it were a SQL database. It translates JDBC calls into Redash API requests, enabling you to query Redash data sources directly from your Java applications.

## Features

- Connect to Redash using standard JDBC connection strings
- Execute SQL queries against Redash data sources
- Read-only operations (SELECT and some read queries)
- Support for query parameters
- Connection pooling

## Connection URL Format

```
jdbc:redash://<redash-host>:<port>?apiKey=<your-api-key>
```

Example:
```
jdbc:redash://redash.example.com:80?apiKey=key123
```

## Usage Example

```java
// Load the driver
Class.forName("com.manu156.driver.redash.RedashDriver");

// Create a connection
String url = "jdbc:redash://redash.example.com:80?apiKey=your_api_key";
Connection conn = DriverManager.getConnection(url);

// Execute a query
Statement stmt = conn.createStatement();
ResultSet rs = stmt.executeQuery("SELECT * FROM table1 limit 10;");

// Process results
while (rs.next()) {
    String value = rs.getString("column_name");
    // Process data...
}

// Close resources
rs.close();
stmt.close();
conn.close();
```

## Building

To build the driver:

```
mvn clean package
```

This will create a JAR file in the `target` directory that you can include in your Java applications.

## Limitations

- This driver is read-only and does not support INSERT, UPDATE, or DELETE operations
- It requires a valid Redash API key with appropriate permissions
- Query execution is dependent on the Redash API's performance and limitations

