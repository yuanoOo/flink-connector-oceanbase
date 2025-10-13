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

package com.oceanbase.connector.flink;

import com.oceanbase.connector.flink.utils.OptionUtils;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public class OBKVHBaseConnectorOptions extends ConnectorOptions {

    private static final long serialVersionUID = 1L;

    public static final ConfigOption<String> SYS_USERNAME =
            ConfigOptions.key("sys.username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username of system tenant.");

    public static final ConfigOption<String> SYS_PASSWORD =
            ConfigOptions.key("sys.password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password of system tenant");

    public static final ConfigOption<String> HBASE_PROPERTIES =
            ConfigOptions.key("hbase.properties")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Properties to configure 'obkv-hbase-client-java'.");

    public static final ConfigOption<Boolean> ODP_MODE =
            ConfigOptions.key("odp-mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to use ODP to connect to OBKV.");

    public static final ConfigOption<String> ODP_IP =
            ConfigOptions.key("odp-ip")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ODP IP address.");

    public static final ConfigOption<Integer> ODP_PORT =
            ConfigOptions.key("odp-port")
                    .intType()
                    .defaultValue(2885)
                    .withDescription("ODP rpc port.");

    // Source related options
    public static final ConfigOption<Boolean> LOOKUP_CACHE_ENABLED =
            ConfigOptions.key("lookup.cache.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable lookup cache.");

    public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS =
            ConfigOptions.key("lookup.cache.max-rows")
                    .longType()
                    .defaultValue(10000L)
                    .withDescription("Max rows of lookup cache.");

    public static final ConfigOption<Duration> LOOKUP_CACHE_TTL =
            ConfigOptions.key("lookup.cache.ttl")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription("TTL of lookup cache.");

    public static final ConfigOption<Integer> SCAN_CACHING =
            ConfigOptions.key("scan.caching")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("The number of rows fetched per RPC when scanning.");

    public OBKVHBaseConnectorOptions(Map<String, String> config) {
        super(config);
    }

    public String getSysUsername() {
        return allConfig.get(SYS_USERNAME);
    }

    public String getSysPassword() {
        return allConfig.get(SYS_PASSWORD);
    }

    public Properties getHBaseProperties() {
        return OptionUtils.parseProperties(allConfig.get(HBASE_PROPERTIES));
    }

    public Boolean getOdpMode() {
        return allConfig.get(ODP_MODE);
    }

    public String getOdpIP() {
        return allConfig.get(ODP_IP);
    }

    public Integer getOdpPort() {
        return allConfig.get(ODP_PORT);
    }

    public Boolean getLookupCacheEnabled() {
        return allConfig.get(LOOKUP_CACHE_ENABLED);
    }

    public Long getLookupCacheMaxRows() {
        return allConfig.get(LOOKUP_CACHE_MAX_ROWS);
    }

    public Duration getLookupCacheTTL() {
        return allConfig.get(LOOKUP_CACHE_TTL);
    }

    public Integer getScanCaching() {
        return allConfig.get(SCAN_CACHING);
    }
}
