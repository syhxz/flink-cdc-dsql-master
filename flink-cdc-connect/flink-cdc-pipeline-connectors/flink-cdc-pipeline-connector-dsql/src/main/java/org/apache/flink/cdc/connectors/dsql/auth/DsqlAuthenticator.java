/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.dsql.auth;

import org.apache.flink.cdc.common.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dsql.DsqlUtilities;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.HOST;
import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.IAM_ROLE;
import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.REGION;
import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.USERNAME;
import static org.apache.flink.cdc.connectors.dsql.sink.DsqlSinkOptions.USE_IAM_AUTH;

/**
 * Handles authentication for Amazon DSQL connections.
 * Supports both IAM authentication and username/password authentication.
 */
public class DsqlAuthenticator {
    private static final Logger LOG = LoggerFactory.getLogger(DsqlAuthenticator.class);
    
    // Token cache to avoid regenerating tokens too frequently
    private static final ConcurrentHashMap<String, TokenCacheEntry> TOKEN_CACHE = new ConcurrentHashMap<>();
    
    // Token refresh interval (20 minutes, more frequent refresh for reliability)
    private static final long TOKEN_REFRESH_INTERVAL_MINUTES = 20;
    
    // Scheduled executor for token refresh
    private static final ScheduledExecutorService TOKEN_REFRESH_EXECUTOR = 
        Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "dsql-token-refresh");
            t.setDaemon(true);
            return t;
        });

    private final Configuration config;
    private final boolean useIamAuth;
    private final String region;
    private final String host;
    private final String iamRole;

    public DsqlAuthenticator(Configuration config) {
        this.config = config;
        this.useIamAuth = config.get(USE_IAM_AUTH);
        this.region = config.get(REGION);
        this.host = config.get(HOST);
        this.iamRole = config.get(IAM_ROLE);
        
        LOG.info("Initialized DSQL authenticator with IAM auth: {}, region: {}", useIamAuth, region);
    }

    /**
     * Configures authentication properties for DSQL connection.
     *
     * @param properties The connection properties to configure
     */
    public void configureAuthentication(Properties properties) {
        if (useIamAuth) {
            configureIamAuthentication(properties);
        } else {
            configureUsernamePasswordAuthentication(properties);
        }
    }

    /**
     * Configures username/password authentication.
     */
    private void configureUsernamePasswordAuthentication(Properties properties) {
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);

        if (username == null || password == null) {
            throw new IllegalArgumentException(
                "Username and password are required when IAM authentication is disabled");
        }

        properties.setProperty("user", username);
        properties.setProperty("password", password);
        
        LOG.debug("Configured username/password authentication for user: {}", username);
    }

    /**
     * Configures IAM authentication using AWS SDK and DSQL utilities.
     */
    private void configureIamAuthentication(Properties properties) {
        if (region == null || region.trim().isEmpty()) {
            throw new IllegalArgumentException("Region is required for IAM authentication");
        }

        if (host == null || host.trim().isEmpty()) {
            throw new IllegalArgumentException("Host is required for IAM authentication");
        }

        try {
            // Generate or get cached authentication token
            String authToken = getOrGenerateAuthToken();
            
            // Configure connection properties for IAM authentication
            properties.setProperty("user", "admin"); // DSQL admin user for IAM auth
            properties.setProperty("password", authToken);
            
            LOG.info("Configured IAM authentication for DSQL cluster: {}", host);
            
        } catch (Exception e) {
            LOG.error("Failed to configure IAM authentication: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to configure IAM authentication", e);
        }
    }

    /**
     * Gets an existing valid token from cache or generates a new one.
     */
    private String getOrGenerateAuthToken() {
        String cacheKey = host + ":" + region + ":" + (iamRole != null ? iamRole : "default");
        
        TokenCacheEntry cachedEntry = TOKEN_CACHE.get(cacheKey);
        
        // Check if we have a valid cached token
        if (cachedEntry != null && !cachedEntry.isExpired()) {
            LOG.debug("Using cached authentication token for {} (age: {}min)", 
                     host, cachedEntry.getAgeMinutes());
            return cachedEntry.getToken();
        }
        
        // Log token expiration or missing token
        if (cachedEntry != null) {
            LOG.info("Cached token expired for {} (age: {}min), generating new token", 
                    host, cachedEntry.getAgeMinutes());
        } else {
            LOG.info("No cached token found for {}, generating new token", host);
        }
        
        // Generate new token with retry logic
        String newToken = generateAuthTokenWithRetry();
        
        // Cache the new token
        TokenCacheEntry newEntry = new TokenCacheEntry(newToken, System.currentTimeMillis());
        TOKEN_CACHE.put(cacheKey, newEntry);
        
        // Schedule token refresh
        scheduleTokenRefresh(cacheKey);
        
        LOG.info("Generated and cached new authentication token for DSQL cluster: {}", host);
        return newToken;
    }

    /**
     * Generates a new authentication token with retry logic.
     */
    private String generateAuthTokenWithRetry() {
        int maxRetries = 3;
        Exception lastException = null;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return generateAuthToken();
            } catch (Exception e) {
                lastException = e;
                LOG.warn("Failed to generate DSQL auth token (attempt {}/{}): {}", 
                        attempt, maxRetries, e.getMessage());
                
                if (attempt < maxRetries) {
                    try {
                        // Exponential backoff: 1s, 2s, 4s
                        long delayMs = 1000L * (1L << (attempt - 1));
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during token generation retry", ie);
                    }
                }
            }
        }
        
        throw new RuntimeException("Failed to generate DSQL authentication token after " + 
                                 maxRetries + " attempts", lastException);
    }

    /**
     * Generates a new authentication token using AWS SDK.
     */
    private String generateAuthToken() {
        try {
            Region awsRegion = Region.of(region);
            
            DsqlUtilities utilities = DsqlUtilities.builder()
                    .region(awsRegion)
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build();

            // Generate admin auth token for DSQL
            String token = utilities.generateDbConnectAdminAuthToken(builder -> {
                builder.hostname(host)
                        .region(awsRegion);
            });

            LOG.debug("Successfully generated DSQL authentication token");
            return token;
            
        } catch (Exception e) {
            LOG.error("Failed to generate DSQL authentication token: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to generate DSQL authentication token", e);
        }
    }

    /**
     * Schedules automatic token refresh before expiration.
     */
    private void scheduleTokenRefresh(String cacheKey) {
        TOKEN_REFRESH_EXECUTOR.schedule(() -> {
            try {
                LOG.debug("Refreshing authentication token for cache key: {}", cacheKey);
                
                // Generate new token
                String newToken = generateAuthToken();
                
                // Update cache
                TokenCacheEntry newEntry = new TokenCacheEntry(newToken, System.currentTimeMillis());
                TOKEN_CACHE.put(cacheKey, newEntry);
                
                // Schedule next refresh
                scheduleTokenRefresh(cacheKey);
                
                LOG.info("Successfully refreshed authentication token for: {}", cacheKey);
                
            } catch (Exception e) {
                LOG.error("Failed to refresh authentication token for {}: {}", cacheKey, e.getMessage(), e);
                // Remove invalid entry from cache
                TOKEN_CACHE.remove(cacheKey);
            }
        }, TOKEN_REFRESH_INTERVAL_MINUTES, TimeUnit.MINUTES);
    }

    /**
     * Checks if IAM authentication is enabled.
     */
    public boolean isIamAuthEnabled() {
        return useIamAuth;
    }

    /**
     * Forces a token refresh by clearing the cache and generating a new token.
     * This method is useful when a token expiration error is detected.
     */
    public String forceTokenRefresh() {
        String cacheKey = host + ":" + region + ":" + (iamRole != null ? iamRole : "default");
        
        LOG.info("Forcing token refresh for DSQL cluster: {}", host);
        
        // Remove expired token from cache
        TOKEN_CACHE.remove(cacheKey);
        
        // Generate new token
        return getOrGenerateAuthToken();
    }

    /**
     * Clears the token cache (useful for testing or manual refresh).
     */
    public static void clearTokenCache() {
        TOKEN_CACHE.clear();
        LOG.info("Cleared DSQL authentication token cache");
    }

    /**
     * Gets cache statistics for monitoring.
     */
    public static String getCacheStats() {
        StringBuilder stats = new StringBuilder();
        stats.append("DSQL Token Cache Stats: ");
        stats.append("entries=").append(TOKEN_CACHE.size());
        
        for (Map.Entry<String, TokenCacheEntry> entry : TOKEN_CACHE.entrySet()) {
            stats.append(", ").append(entry.getKey())
                 .append("(age=").append(entry.getValue().getAgeMinutes()).append("min")
                 .append(",expired=").append(entry.getValue().isExpired()).append(")");
        }
        
        return stats.toString();
    }

    /**
     * Shuts down the token refresh executor.
     */
    public static void shutdown() {
        TOKEN_REFRESH_EXECUTOR.shutdown();
        try {
            if (!TOKEN_REFRESH_EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
                TOKEN_REFRESH_EXECUTOR.shutdownNow();
            }
        } catch (InterruptedException e) {
            TOKEN_REFRESH_EXECUTOR.shutdownNow();
            Thread.currentThread().interrupt();
        }
        LOG.info("Shut down DSQL token refresh executor");
    }

    /**
     * Cache entry for authentication tokens.
     */
    private static class TokenCacheEntry {
        private final String token;
        private final long createdAt;
        
        // More conservative token validity - refresh every 25 minutes instead of 50
        private static final long TOKEN_VALIDITY_MINUTES = 25;
        
        public TokenCacheEntry(String token, long createdAt) {
            this.token = token;
            this.createdAt = createdAt;
        }
        
        public String getToken() {
            return token;
        }
        
        public boolean isExpired() {
            long ageMinutes = (System.currentTimeMillis() - createdAt) / (1000 * 60);
            boolean expired = ageMinutes >= TOKEN_VALIDITY_MINUTES;
            
            if (expired) {
                LOG.debug("Token expired: age={}min, validity={}min", ageMinutes, TOKEN_VALIDITY_MINUTES);
            }
            
            return expired;
        }
        
        public long getAgeMinutes() {
            return (System.currentTimeMillis() - createdAt) / (1000 * 60);
        }
    }
}
