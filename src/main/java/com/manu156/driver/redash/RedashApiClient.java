package com.manu156.driver.redash;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Client for interacting with the Redash API.
 */
public class RedashApiClient {
    private static final Logger logger = LoggerFactory.getLogger(RedashApiClient.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private final String baseUrl;
    private final String apiKey;
    private final CloseableHttpClient httpClient;
    private final String host;
    private final int port;
    
    public RedashApiClient(String host, int port, String apiKey) {
        this.host = host;
        this.port = port;
        this.baseUrl = "http://" + host + ":" + port + "/api";
        this.apiKey = apiKey;
        this.httpClient = HttpClientBuilder.create().build();
    }
    
    /**
     * Test the connection to the Redash API by attempting to list data sources.
     * 
     * @param timeoutSeconds Timeout in seconds
     * @return null if connection is successful, error message otherwise
     */
    public String testConnection(int timeoutSeconds) {
        try {
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout((int) TimeUnit.SECONDS.toMillis(timeoutSeconds))
                    .setSocketTimeout((int) TimeUnit.SECONDS.toMillis(timeoutSeconds))
                    .build();
            
            String dataSourcesUrl = baseUrl + "/data_sources";
            logger.info("Testing connection to Redash API at URL: {}", dataSourcesUrl);
            
            HttpGet request = new HttpGet(dataSourcesUrl);
            request.setConfig(requestConfig);
            request.setHeader("Authorization", "Key " + apiKey);
            
            HttpResponse response = httpClient.execute(request);
            int statusCode = response.getStatusLine().getStatusCode();
            String responseBody = EntityUtils.toString(response.getEntity());
            
            logger.info("Received response with status code: {}", statusCode);
            
            if (statusCode == 200) {
                JsonNode responseNode = objectMapper.readTree(responseBody);
                if (responseNode.isArray()) {
                    return null; // Connection successful
                }
                return "Invalid response format from server";
            } else if (statusCode == 401 || statusCode == 403) {
                return "Authentication failed - please check your API key";
            } else {
                return String.format("Connection failed with status code %d: %s", statusCode, responseBody);
            }
        } catch (Exception e) {
            logger.error("Error testing connection to Redash API", e);
            return "Connection error: " + e.getMessage();
        }
    }
    
    /**
     * Execute a query by ID and return the results.
     * 
     * @param queryId The ID of the query to execute
     * @param parameters Query parameters
     * @return Query results
     * @throws SQLException if there's an error executing the query
     */
    public RedashQueryResult executeQueryById(String queryId, Map<String, Object> parameters) throws SQLException {
        long startTime = System.currentTimeMillis();
        try {
            // First, get the query details
            logger.debug("Fetching query details for ID: {}", queryId);
            HttpGet queryRequest = new HttpGet(baseUrl + "/queries/" + queryId);
            queryRequest.setHeader("Authorization", "Key " + apiKey);
            
            long requestStartTime = System.currentTimeMillis();
            HttpResponse queryResponse = httpClient.execute(queryRequest);
            String queryJson = EntityUtils.toString(queryResponse.getEntity());
            long requestEndTime = System.currentTimeMillis();
            logger.debug("Query details fetch took {} ms", requestEndTime - requestStartTime);
            
            if (queryResponse.getStatusLine().getStatusCode() != 200) {
                throw new SQLException("Failed to get query details: " + queryJson);
            }
            
            JsonNode queryNode = objectMapper.readTree(queryJson);
            String dataSourceId = queryNode.path("data_source_id").asText();
            String queryText = queryNode.path("query").asText();
            
            logger.debug("Retrieved query text:\n{}", queryText);
            logger.debug("Executing query: [length: {} chars] against data source: {}", 
                       queryText.length(), dataSourceId);
            
            // Now execute the query
            RedashQueryResult result = executeQuery(dataSourceId, queryText, parameters);
            long totalTime = System.currentTimeMillis() - startTime;
            logger.debug("Total executeQueryById operation took {} ms", totalTime);
            return result;
        } catch (IOException e) {
            throw new SQLException("Error executing query by ID", e);
        }
    }
    
    /**
     * Execute a raw SQL query against a specific data source.
     * 
     * @param dataSourceId The ID of the data source to query
     * @param query The SQL query to execute
     * @param parameters Query parameters
     * @return Query results
     * @throws SQLException if there's an error executing the query
     */
    public RedashQueryResult executeQuery(String dataSourceId, String query, Map<String, Object> parameters) throws SQLException {
        long startTime = System.currentTimeMillis();
        try {
            if (parameters != null && !parameters.isEmpty()) {
                logger.info("With parameters: {}", parameters);
            }
            
            // Prepare query data
            long prepStartTime = System.currentTimeMillis();
            Map<String, Object> queryData = new HashMap<>();
            queryData.put("data_source_id", dataSourceId);
            queryData.put("query", query);
            if (parameters != null && !parameters.isEmpty()) {
                queryData.put("parameters", parameters);
            }
            
            HttpPost request = new HttpPost(baseUrl + "/query_results");
            request.setHeader("Authorization", "Key " + apiKey);
            request.setHeader("Content-Type", "application/json");
            
            String jsonBody = objectMapper.writeValueAsString(queryData);
            request.setEntity(new StringEntity(jsonBody));
            long prepEndTime = System.currentTimeMillis();
            logger.debug("Query preparation took {} ms", prepEndTime - prepStartTime);
            
            // Execute query
            long execStartTime = System.currentTimeMillis();
            HttpResponse response = httpClient.execute(request);
            HttpEntity entity = response.getEntity();
            String responseJson = EntityUtils.toString(entity);
            long execEndTime = System.currentTimeMillis();
            logger.debug("Initial query request took {} ms", execEndTime - execStartTime);
            
            if (response.getStatusLine().getStatusCode() != 200) {
                logger.error("Query execution failed: {}", responseJson);
                logger.error("Url: {}", baseUrl + "/query_results");
                throw new SQLException("Query execution failed: " + responseJson);
            }
            
            JsonNode rootNode = objectMapper.readTree(responseJson);
            JsonNode jobNode = rootNode.path("job");
            
            // Check if the query is still running
            if (jobNode != null && !jobNode.isMissingNode()) {
                String jobId = jobNode.path("id").asText();
                logger.info("Query executing asynchronously with job ID: {}", jobId);
                RedashQueryResult result = waitForQueryResults(jobId);
                long totalTime = System.currentTimeMillis() - startTime;
                logger.info("Total query execution (including async wait) took {} ms", totalTime);
                return result;
            }
            
            // If we have results immediately
            long parseStartTime = System.currentTimeMillis();
            RedashQueryResult result = parseQueryResults(rootNode);
            long parseEndTime = System.currentTimeMillis();
            logger.debug("Result parsing took {} ms", parseEndTime - parseStartTime);
            
            long totalTime = System.currentTimeMillis() - startTime;
            logger.debug("Total synchronous query execution took {} ms", totalTime);
            return result;
        } catch (IOException e) {
            throw new SQLException("Error executing query", e);
        }
    }
    
    /**
     * Wait for query results to be available.
     * 
     * @param jobId The ID of the query job
     * @return Query results
     * @throws SQLException if there's an error getting the results
     */
    private RedashQueryResult waitForQueryResults(String jobId) throws SQLException {
        try {
            int maxAttempts = 60; // Wait up to 5 minutes (60 * 5 seconds)
            int attempt = 0;
            long waitStartTime = System.currentTimeMillis();
            
            while (attempt < maxAttempts) {
                HttpGet request = new HttpGet(baseUrl + "/jobs/" + jobId);
                request.setHeader("Authorization", "Key " + apiKey);
                
                long pollStartTime = System.currentTimeMillis();
                HttpResponse response = httpClient.execute(request);
                String responseJson = EntityUtils.toString(response.getEntity());
                long pollEndTime = System.currentTimeMillis();
                
                if (response.getStatusLine().getStatusCode() != 200) {
                    throw new SQLException("Failed to get job status: " + responseJson);
                }
                
                JsonNode rootNode = objectMapper.readTree(responseJson);
                JsonNode jobNode = rootNode.path("job");
                String status = jobNode.path("status").asText();
                
                logger.info("Job {} status check #{}: {} (poll took {} ms)", 
                           jobId, attempt + 1, status, pollEndTime - pollStartTime);
                
                if ("finished".equals(status)) {
                    String queryResultId = jobNode.path("query_result_id").asText();
                    long totalWaitTime = System.currentTimeMillis() - waitStartTime;
                    logger.info("Query completed after {} attempts, total wait time: {} ms", 
                              attempt + 1, totalWaitTime);
                    return getQueryResultById(queryResultId);
                } else if ("failed".equals(status)) {
                    String error = jobNode.path("error").asText();
                    throw new SQLException("Query execution failed: " + error);
                }
                
                // Wait before checking again
                Thread.sleep(5000); // 5 seconds
                attempt++;
            }
            
            throw new SQLException("Query execution timed out after " + maxAttempts + " attempts");
        } catch (IOException | InterruptedException e) {
            throw new SQLException("Error waiting for query results", e);
        }
    }
    
    /**
     * Get query results by ID.
     * 
     * @param queryResultId The ID of the query result
     * @return Query results
     * @throws SQLException if there's an error getting the results
     */
    private RedashQueryResult getQueryResultById(String queryResultId) throws SQLException {
        try {
            HttpGet request = new HttpGet(baseUrl + "/query_results/" + queryResultId);
            request.setHeader("Authorization", "Key " + apiKey);
            
            HttpResponse response = httpClient.execute(request);
            String responseJson = EntityUtils.toString(response.getEntity());
            
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new SQLException("Failed to get query results: " + responseJson);
            }
            
            JsonNode rootNode = objectMapper.readTree(responseJson);
            return parseQueryResults(rootNode);
        } catch (IOException e) {
            throw new SQLException("Error getting query results", e);
        }
    }
    
    /**
     * Parse query results from JSON.
     * 
     * @param rootNode The root JSON node
     * @return Query results
     * @throws SQLException if there's an error parsing the results
     */
    private RedashQueryResult parseQueryResults(JsonNode rootNode) throws SQLException {
        try {
            JsonNode queryResultNode = rootNode.path("query_result");
            if (queryResultNode.isMissingNode()) {
                throw new SQLException("No query results found in response");
            }
            
            JsonNode dataNode = queryResultNode.path("data");
            if (dataNode.isMissingNode()) {
                throw new SQLException("No data found in query results");
            }
            
            JsonNode columnsNode = dataNode.path("columns");
            JsonNode rowsNode = dataNode.path("rows");
            
            if (columnsNode.isMissingNode() || rowsNode.isMissingNode()) {
                throw new SQLException("Invalid query results format");
            }
            
            // Parse columns
            List<RedashColumn> columns = new ArrayList<>();
            for (JsonNode columnNode : columnsNode) {
                String name = columnNode.path("name").asText();
                String type = columnNode.path("type").asText();
                columns.add(new RedashColumn(name, type));
            }
            
            // Parse rows
            List<Map<String, Object>> rows = new ArrayList<>();
            for (JsonNode rowNode : rowsNode) {
                Map<String, Object> row = new HashMap<>();
                for (RedashColumn column : columns) {
                    JsonNode valueNode = rowNode.path(column.getName());
                    Object value = null;
                    
                    if (!valueNode.isMissingNode() && !valueNode.isNull()) {
                        switch (column.getType()) {
                            case "integer":
                                value = valueNode.asInt();
                                break;
                            case "float":
                                value = valueNode.asDouble();
                                break;
                            case "boolean":
                                value = valueNode.asBoolean();
                                break;
                            case "string":
                            case "datetime":
                            case "date":
                            default:
                                value = valueNode.asText();
                                break;
                        }
                    }
                    
                    row.put(column.getName(), value);
                }
                rows.add(row);
            }
            
            return new RedashQueryResult(columns, rows);
        } catch (Exception e) {
            throw new SQLException("Error parsing query results", e);
        }
    }
    
    /**
     * Get a list of data sources.
     * 
     * @return List of data sources
     * @throws SQLException if there's an error getting the data sources
     */
    public List<Map<String, String>> getDataSources() throws SQLException {
        try {
            HttpGet request = new HttpGet(baseUrl + "/data_sources");
            request.setHeader("Authorization", "Key " + apiKey);
            
            HttpResponse response = httpClient.execute(request);
            String responseJson = EntityUtils.toString(response.getEntity());
            
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new SQLException("Failed to get data sources: " + responseJson);
            }
            
            JsonNode rootNode = objectMapper.readTree(responseJson);
            List<Map<String, String>> dataSources = new ArrayList<>();
            
            for (JsonNode dataSourceNode : rootNode) {
                Map<String, String> dataSource = new HashMap<>();
                dataSource.put("id", dataSourceNode.path("id").asText());
                dataSource.put("name", dataSourceNode.path("name").asText());
                dataSource.put("type", dataSourceNode.path("type").asText());
                dataSources.add(dataSource);
            }
            
            return dataSources;
        } catch (IOException e) {
            throw new SQLException("Error getting data sources", e);
        }
    }
    
    /**
     * Get a list of queries.
     * 
     * @return List of queries
     * @throws SQLException if there's an error getting the queries
     */
    public List<Map<String, String>> getQueries() throws SQLException {
        try {
            HttpGet request = new HttpGet(baseUrl + "/queries");
            request.setHeader("Authorization", "Key " + apiKey);
            
            HttpResponse response = httpClient.execute(request);
            String responseJson = EntityUtils.toString(response.getEntity());
            
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new SQLException("Failed to get queries: " + responseJson);
            }
            
            JsonNode rootNode = objectMapper.readTree(responseJson);
            JsonNode resultsNode = rootNode.path("results");
            
            List<Map<String, String>> queries = new ArrayList<>();
            
            for (JsonNode queryNode : resultsNode) {
                Map<String, String> query = new HashMap<>();
                query.put("id", queryNode.path("id").asText());
                query.put("name", queryNode.path("name").asText());
                query.put("description", queryNode.path("description").asText());
                query.put("data_source_id", queryNode.path("data_source_id").asText());
                queries.add(query);
            }
            
            return queries;
        } catch (IOException e) {
            throw new SQLException("Error getting queries", e);
        }
    }
    
    /**
     * Create a new query in Redash.
     * 
     * @param name The name of the query
     * @param description The description of the query
     * @param dataSourceId The ID of the data source
     * @param queryText The SQL query text
     * @return The ID of the created query
     * @throws SQLException if there's an error creating the query
     */
    public String createQuery(String name, String description, String dataSourceId, String queryText) throws SQLException {
        try {
            Map<String, Object> queryData = new HashMap<>();
            queryData.put("name", name);
            queryData.put("description", description);
            queryData.put("data_source_id", dataSourceId);
            queryData.put("query", queryText);
            
            HttpPost request = new HttpPost(baseUrl + "/queries");
            request.setHeader("Authorization", "Key " + apiKey);
            request.setHeader("Content-Type", "application/json");
            
            String jsonBody = objectMapper.writeValueAsString(queryData);
            request.setEntity(new StringEntity(jsonBody));
            
            HttpResponse response = httpClient.execute(request);
            String responseJson = EntityUtils.toString(response.getEntity());
            
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new SQLException("Failed to create query: " + responseJson);
            }
            
            JsonNode rootNode = objectMapper.readTree(responseJson);
            return rootNode.path("id").asText();
        } catch (IOException e) {
            throw new SQLException("Error creating query", e);
        }
    }
    
    /**
     * Close the HTTP client.
     */
    public void close() {
        try {
            if (httpClient != null) {
                httpClient.close();
            }
        } catch (IOException e) {
            logger.error("Error closing HTTP client", e);
        }
    }

    public String getHost() {
        return host;
    }

    public String getPort() {
        return Integer.toString(port);
    }
} 