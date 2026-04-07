/*
 * Copyright 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oceanbase.connector.flink.source;

import com.alibaba.druid.pool.DruidDataSource;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/** Configuration for OceanBase source. */
public class OceanBaseSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String url;
    private final String username;
    private final String password;
    private final String schemaName;
    private final String tableName;
    private final String compatibleMode;
    private final int splitSize;
    private final String chunkKeyColumn;
    private final int fetchSize;
    private final boolean oracleTenantCaseInsensitive;
    private final String driverClassName;
    private final String druidProperties;

    /** Backward-compatible constructor with original 9 parameters. */
    public OceanBaseSourceConfig(
            String url,
            String username,
            String password,
            String schemaName,
            String tableName,
            String compatibleMode,
            int splitSize,
            String chunkKeyColumn,
            int fetchSize) {
        this(
                url,
                username,
                password,
                schemaName,
                tableName,
                compatibleMode,
                splitSize,
                chunkKeyColumn,
                fetchSize,
                true,
                null,
                null);
    }

    public OceanBaseSourceConfig(
            String url,
            String username,
            String password,
            String schemaName,
            String tableName,
            String compatibleMode,
            int splitSize,
            String chunkKeyColumn,
            int fetchSize,
            boolean oracleTenantCaseInsensitive,
            String driverClassName,
            String druidProperties) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.compatibleMode = compatibleMode;
        this.splitSize = splitSize;
        this.chunkKeyColumn = chunkKeyColumn;
        this.fetchSize = fetchSize;
        this.oracleTenantCaseInsensitive = oracleTenantCaseInsensitive;
        this.driverClassName = driverClassName;
        this.druidProperties = druidProperties;
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getCompatibleMode() {
        return compatibleMode;
    }

    public boolean isMySqlMode() {
        return "MySQL".equalsIgnoreCase(compatibleMode);
    }

    public boolean isOracleMode() {
        return "Oracle".equalsIgnoreCase(compatibleMode);
    }

    public int getSplitSize() {
        return splitSize;
    }

    public String getChunkKeyColumn() {
        return chunkKeyColumn;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public boolean isOracleTenantCaseInsensitive() {
        return oracleTenantCaseInsensitive;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public String quoteIdentifier(String identifier) {
        if (isOracleMode()) {
            if (oracleTenantCaseInsensitive) {
                return identifier;
            }
            return "\"" + identifier.replaceAll("\"", "\"\"") + "\"";
        } else {
            return "`" + identifier.replaceAll("`", "``") + "`";
        }
    }

    /**
     * Creates a DruidDataSource with default JDBC properties matching the sink-side
     * OceanBaseConnectionProvider behavior.
     */
    public DruidDataSource createConfiguredDataSource() {
        DruidDataSource ds = new DruidDataSource();
        ds.setUrl(url);
        ds.setUsername(username);
        ds.setPassword(password);
        if (driverClassName != null && !driverClassName.isEmpty()) {
            ds.setDriverClassName(driverClassName);
        }
        ds.setConnectProperties(initializeDefaultJdbcProperties(url));
        if (druidProperties != null && !druidProperties.isEmpty()) {
            Properties props = parseProperties(druidProperties);
            ds.configFromPropeties(props);
        }
        return ds;
    }

    private static Properties initializeDefaultJdbcProperties(String jdbcUrl) {
        Properties defaultJdbcProperties = new Properties();
        defaultJdbcProperties.setProperty("useSSL", "false");
        defaultJdbcProperties.setProperty("rewriteBatchedStatements", "true");
        defaultJdbcProperties.setProperty("initialTimeout", "2");
        defaultJdbcProperties.setProperty("autoReconnect", "true");
        defaultJdbcProperties.setProperty("maxReconnects", "3");
        defaultJdbcProperties.setProperty("useInformationSchema", "true");
        defaultJdbcProperties.setProperty("nullCatalogMeansCurrent", "false");
        defaultJdbcProperties.setProperty("useUnicode", "true");
        defaultJdbcProperties.setProperty("zeroDateTimeBehavior", "convertToNull");
        defaultJdbcProperties.setProperty("characterEncoding", "UTF-8");
        defaultJdbcProperties.setProperty("characterSetResults", "UTF-8");

        // Avoid overwriting user's custom jdbc properties in the URL.
        List<String> jdbcUrlProperties =
                defaultJdbcProperties.keySet().stream()
                        .map(Object::toString)
                        .filter(jdbcUrl::contains)
                        .collect(Collectors.toList());
        jdbcUrlProperties.forEach(defaultJdbcProperties::remove);

        return defaultJdbcProperties;
    }

    private static Properties parseProperties(String propsStr) {
        Properties props = new Properties();
        if (propsStr == null || propsStr.isEmpty()) {
            return props;
        }
        for (String kv : propsStr.split(";")) {
            String trimmed = kv.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int idx = trimmed.indexOf('=');
            if (idx > 0) {
                props.setProperty(
                        trimmed.substring(0, idx).trim(), trimmed.substring(idx + 1).trim());
            }
        }
        return props;
    }
}
