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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class OceanBaseConnectorOptions extends ConnectorOptions {
    private static final long serialVersionUID = 1L;

    public static final ConfigOption<String> DRIVER_CLASS_NAME =
            ConfigOptions.key("driver-class-name")
                    .stringType()
                    .defaultValue("com.mysql.cj.jdbc.Driver")
                    .withDescription(
                            "JDBC driver class name, use 'com.mysql.cj.jdbc.Driver' by default.");

    public static final ConfigOption<String> DRUID_PROPERTIES =
            ConfigOptions.key("druid-properties")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Properties for specific connection pool.");

    public static final ConfigOption<Boolean> MEMSTORE_CHECK_ENABLED =
            ConfigOptions.key("memstore-check.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether enable memstore check. Default value is 'true'");

    public static final ConfigOption<Double> MEMSTORE_THRESHOLD =
            ConfigOptions.key("memstore-check.threshold")
                    .doubleType()
                    .defaultValue(0.9)
                    .withDescription(
                            "Memory usage threshold ratio relative to the limit value. Default value is '0.9'.");

    public static final ConfigOption<Duration> MEMSTORE_CHECK_INTERVAL =
            ConfigOptions.key("memstore-check.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The check interval, over this time, the writer will check if memstore reaches threshold. Default value is '30s'.");

    public static final ConfigOption<Boolean> PARTITION_ENABLED =
            ConfigOptions.key("partition.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to enable partition calculation and flush records by partitions. Default value is 'false'.");

    public static final ConfigOption<Boolean> TABLE_ORACLE_TENANT_CASE_INSENSITIVE =
            ConfigOptions.key("table.oracle-tenant-case-insensitive")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "By default, under the Oracle tenant, schema names and column names are case-insensitive.");

    public static final ConfigOption<String> FILE_COMPLETION_FLAG_COLUMN =
            ConfigOptions.key("file-completion.flag-column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Physical column name marking the last row of an OSS file. When this column evaluates true, a Kafka notification is sent after the row is flushed to OceanBase.");

    public static final ConfigOption<String> FILE_COMPLETION_MESSAGE_COLUMN =
            ConfigOptions.key("file-completion.message-column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Physical column whose string value is sent as the Kafka record value for file completion.");

    public static final ConfigOption<String> FILE_COMPLETION_KAFKA_TOPIC =
            ConfigOptions.key("file-completion.kafka.topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kafka topic for file-completion notifications.");

    /**
     * When {@code true}, all required file-completion Kafka options must be set or validation
     * fails. Default is {@code false}: no Kafka notifications and other file-completion keys are
     * not validated.
     */
    public static final ConfigOption<Boolean> FILE_COMPLETION_KAFKA_NOTIFICATION_ENABLED =
            ConfigOptions.key("file-completion.kafka.notification-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to send Kafka notifications on file completion. Default false. "
                                    + "When true, all required file-completion options must be set.");

    /**
     * All keys with this prefix are forwarded to the Kafka producer config (prefix stripped). Use
     * to set bootstrap.servers, security.protocol, sasl.*, etc.
     */
    public static final String FILE_COMPLETION_KAFKA_PROPS_PREFIX =
            "file-completion.kafka.properties.";

    private static final String KAFKA_BOOTSTRAP_SERVERS = "bootstrap.servers";

    private final Map<String, String> rawOptions;

    public OceanBaseConnectorOptions(Map<String, String> config) {
        super(config);
        this.rawOptions = config;
    }

    public String getDriverClassName() {
        return allConfig.get(DRIVER_CLASS_NAME);
    }

    public Properties getDruidProperties() {
        return OptionUtils.parseProperties(allConfig.get(DRUID_PROPERTIES));
    }

    public boolean getMemStoreCheckEnabled() {
        return allConfig.get(MEMSTORE_CHECK_ENABLED);
    }

    public double getMemStoreThreshold() {
        return allConfig.get(MEMSTORE_THRESHOLD);
    }

    public long getMemStoreCheckInterval() {
        return allConfig.get(MEMSTORE_CHECK_INTERVAL).toMillis();
    }

    public boolean getPartitionEnabled() {
        return allConfig.get(PARTITION_ENABLED);
    }

    public boolean getTableOracleTenantCaseInsensitive() {
        return allConfig.get(TABLE_ORACLE_TENANT_CASE_INSENSITIVE);
    }

    /** True when notification switch is on and all required file-completion options are set. */
    public boolean isFileCompletionKafkaEnabled() {
        return isFileCompletionKafkaNotificationEnabled()
                && hasAllFileCompletionKafkaRequiredOptions();
    }

    public boolean isFileCompletionKafkaNotificationEnabled() {
        return allConfig.get(FILE_COMPLETION_KAFKA_NOTIFICATION_ENABLED);
    }

    private boolean hasAllFileCompletionKafkaRequiredOptions() {
        return getFileCompletionFlagColumn() != null
                && getFileCompletionMessageColumn() != null
                && getFileCompletionKafkaTopic() != null
                && kafkaProperty(KAFKA_BOOTSTRAP_SERVERS) != null;
    }

    public String getFileCompletionFlagColumn() {
        return trimToNull(allConfig.get(FILE_COMPLETION_FLAG_COLUMN));
    }

    public String getFileCompletionMessageColumn() {
        return trimToNull(allConfig.get(FILE_COMPLETION_MESSAGE_COLUMN));
    }

    public String getFileCompletionKafkaTopic() {
        return trimToNull(allConfig.get(FILE_COMPLETION_KAFKA_TOPIC));
    }

    /** Returns producer config built from all {@link #FILE_COMPLETION_KAFKA_PROPS_PREFIX} keys. */
    public Properties buildFileCompletionKafkaProducerProperties() {
        Properties props = new Properties();
        for (Map.Entry<String, String> entry : kafkaPropertyView().entrySet()) {
            props.put(entry.getKey(), entry.getValue());
        }
        props.putIfAbsent(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.putIfAbsent(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    /**
     * When notification is enabled, all required file-completion Kafka options must be complete.
     */
    public void validateFileCompletionOptions() {
        if (!isFileCompletionKafkaNotificationEnabled()) {
            return;
        }
        requireCompleteFileCompletionKafkaOptions();
    }

    private void requireCompleteFileCompletionKafkaOptions() {
        String flag = require(FILE_COMPLETION_FLAG_COLUMN.key(), getFileCompletionFlagColumn());
        String msg =
                require(FILE_COMPLETION_MESSAGE_COLUMN.key(), getFileCompletionMessageColumn());
        require(FILE_COMPLETION_KAFKA_TOPIC.key(), getFileCompletionKafkaTopic());
        require(
                FILE_COMPLETION_KAFKA_PROPS_PREFIX + KAFKA_BOOTSTRAP_SERVERS,
                kafkaProperty(KAFKA_BOOTSTRAP_SERVERS));
        if (flag.equalsIgnoreCase(msg)) {
            throw new IllegalArgumentException(
                    "'"
                            + FILE_COMPLETION_FLAG_COLUMN.key()
                            + "' and '"
                            + FILE_COMPLETION_MESSAGE_COLUMN.key()
                            + "' must be different.");
        }
    }

    /** View of options under {@link #FILE_COMPLETION_KAFKA_PROPS_PREFIX} (prefix stripped). */
    private Map<String, String> kafkaPropertyView() {
        Map<String, String> view = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : rawOptions.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(FILE_COMPLETION_KAFKA_PROPS_PREFIX)) {
                continue;
            }
            String value = trimToNull(entry.getValue());
            if (value == null) {
                continue;
            }
            view.put(key.substring(FILE_COMPLETION_KAFKA_PROPS_PREFIX.length()), value);
        }
        return view;
    }

    private String kafkaProperty(String key) {
        return kafkaPropertyView().get(key);
    }

    private static String trimToNull(String s) {
        if (s == null) {
            return null;
        }
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    private static String require(String name, String value) {
        if (value == null) {
            throw new IllegalArgumentException(
                    "Missing required option '"
                            + name
                            + "' for file-completion Kafka notification.");
        }
        return value;
    }
}
