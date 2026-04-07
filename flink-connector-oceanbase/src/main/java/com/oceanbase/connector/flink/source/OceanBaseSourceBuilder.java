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

import org.apache.flink.table.types.DataType;

/** Builder for OceanBaseSource. */
public class OceanBaseSourceBuilder {

    private String url;
    private String username;
    private String password;
    private String schemaName;
    private String tableName;
    private String compatibleMode = "MySQL";
    private int splitSize = 8192;
    private String chunkKeyColumn;
    private int fetchSize = 1024;
    private boolean oracleTenantCaseInsensitive = true;
    private String driverClassName;
    private String druidProperties;
    private DataType producedDataType;

    public OceanBaseSourceBuilder url(String url) {
        this.url = url;
        return this;
    }

    public OceanBaseSourceBuilder username(String username) {
        this.username = username;
        return this;
    }

    public OceanBaseSourceBuilder password(String password) {
        this.password = password;
        return this;
    }

    public OceanBaseSourceBuilder schemaName(String schemaName) {
        this.schemaName = schemaName;
        return this;
    }

    public OceanBaseSourceBuilder tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public OceanBaseSourceBuilder compatibleMode(String compatibleMode) {
        this.compatibleMode = compatibleMode;
        return this;
    }

    public OceanBaseSourceBuilder splitSize(int splitSize) {
        this.splitSize = splitSize;
        return this;
    }

    public OceanBaseSourceBuilder chunkKeyColumn(String chunkKeyColumn) {
        this.chunkKeyColumn = chunkKeyColumn;
        return this;
    }

    public OceanBaseSourceBuilder fetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    public OceanBaseSourceBuilder oracleTenantCaseInsensitive(boolean oracleTenantCaseInsensitive) {
        this.oracleTenantCaseInsensitive = oracleTenantCaseInsensitive;
        return this;
    }

    public OceanBaseSourceBuilder driverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
        return this;
    }

    public OceanBaseSourceBuilder druidProperties(String druidProperties) {
        this.druidProperties = druidProperties;
        return this;
    }

    public OceanBaseSourceBuilder producedDataType(DataType producedDataType) {
        this.producedDataType = producedDataType;
        return this;
    }

    public OceanBaseSource build() {
        OceanBaseSourceConfig config =
                new OceanBaseSourceConfig(
                        url,
                        username,
                        password,
                        schemaName,
                        tableName,
                        compatibleMode,
                        splitSize,
                        chunkKeyColumn,
                        fetchSize,
                        oracleTenantCaseInsensitive,
                        driverClassName,
                        druidProperties);

        return new OceanBaseSource(config, producedDataType);
    }
}
