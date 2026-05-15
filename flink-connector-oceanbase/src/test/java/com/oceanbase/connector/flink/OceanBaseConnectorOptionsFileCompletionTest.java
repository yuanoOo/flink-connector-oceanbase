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

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for file-completion related {@link OceanBaseConnectorOptions} validation. */
class OceanBaseConnectorOptionsFileCompletionTest {

    private static Map<String, String> base() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("url", "jdbc:mysql://127.0.0.1:1/test");
        m.put("username", "u");
        m.put("password", "p");
        m.put("schema-name", "test");
        m.put("table-name", "t");
        return m;
    }

    @Test
    void validateFailsWhenNotificationExplicitlyTrueButOptionsIncomplete() {
        Map<String, String> m = base();
        m.put("file-completion.kafka.notification-enabled", "true");
        m.put("file-completion.flag-column", "is_eof");
        OceanBaseConnectorOptions opts = new OceanBaseConnectorOptions(m);
        assertThrows(IllegalArgumentException.class, opts::validateFileCompletionOptions);
        assertFalse(opts.isFileCompletionKafkaEnabled());
    }

    @Test
    void validatePassesAndKafkaEnabledWhenNotificationExplicitlyTrueAndOptionsComplete() {
        Map<String, String> m = base();
        m.put("file-completion.kafka.notification-enabled", "true");
        m.put("file-completion.flag-column", "is_eof");
        m.put("file-completion.message-column", "kafka_msg");
        m.put("file-completion.kafka.topic", "events");
        m.put("file-completion.kafka.properties.bootstrap.servers", "localhost:9092");
        OceanBaseConnectorOptions opts = new OceanBaseConnectorOptions(m);
        assertDoesNotThrow(opts::validateFileCompletionOptions);
        assertTrue(opts.isFileCompletionKafkaEnabled());
    }

    @Test
    void validatePassesWhenNotificationExplicitlyDisabledEvenIfIncomplete() {
        Map<String, String> m = base();
        m.put("file-completion.kafka.notification-enabled", "false");
        m.put("file-completion.flag-column", "is_eof");
        OceanBaseConnectorOptions opts = new OceanBaseConnectorOptions(m);
        assertDoesNotThrow(opts::validateFileCompletionOptions);
        assertFalse(opts.isFileCompletionKafkaEnabled());
    }

    @Test
    void kafkaDisabledWhenNotificationExplicitlyFalseDespiteFullOptions() {
        Map<String, String> m = base();
        m.put("file-completion.flag-column", "is_eof");
        m.put("file-completion.message-column", "kafka_msg");
        m.put("file-completion.kafka.topic", "events");
        m.put("file-completion.kafka.properties.bootstrap.servers", "localhost:9092");
        m.put("file-completion.kafka.notification-enabled", "false");
        OceanBaseConnectorOptions opts = new OceanBaseConnectorOptions(m);
        assertDoesNotThrow(opts::validateFileCompletionOptions);
        assertFalse(opts.isFileCompletionKafkaEnabled());
    }

    @Test
    void validatePassesWhenNoFileCompletionOptions() {
        OceanBaseConnectorOptions opts = new OceanBaseConnectorOptions(base());
        assertDoesNotThrow(opts::validateFileCompletionOptions);
        assertFalse(opts.isFileCompletionKafkaEnabled());
    }

    @Test
    void validatePassesWhenPartialOptionsWithoutEnablingNotification() {
        Map<String, String> m = base();
        m.put("file-completion.flag-column", "is_eof");
        m.put("file-completion.message-column", "kafka_msg");
        m.put("file-completion.kafka.topic", "events");
        OceanBaseConnectorOptions opts = new OceanBaseConnectorOptions(m);
        assertDoesNotThrow(opts::validateFileCompletionOptions);
        assertFalse(opts.isFileCompletionKafkaEnabled());
    }

    @Test
    void validateFailsWhenNotificationEnabledButBootstrapServersMissing() {
        Map<String, String> m = base();
        m.put("file-completion.kafka.notification-enabled", "true");
        m.put("file-completion.flag-column", "is_eof");
        m.put("file-completion.message-column", "kafka_msg");
        m.put("file-completion.kafka.topic", "events");
        OceanBaseConnectorOptions opts = new OceanBaseConnectorOptions(m);
        IllegalArgumentException ex =
                assertThrows(IllegalArgumentException.class, opts::validateFileCompletionOptions);
        assertTrue(ex.getMessage().contains("bootstrap.servers"), ex.getMessage());
        assertFalse(opts.isFileCompletionKafkaEnabled());
    }

    @Test
    void validateFailsWhenFlagAndMessageColumnNamesEqualIgnoringCase() {
        Map<String, String> m = base();
        m.put("file-completion.kafka.notification-enabled", "true");
        m.put("file-completion.flag-column", "SameCol");
        m.put("file-completion.message-column", "samecol");
        m.put("file-completion.kafka.topic", "events");
        m.put("file-completion.kafka.properties.bootstrap.servers", "localhost:9092");
        OceanBaseConnectorOptions opts = new OceanBaseConnectorOptions(m);
        assertThrows(IllegalArgumentException.class, opts::validateFileCompletionOptions);
    }

    @Test
    void kafkaDisabledByDefaultEvenWhenAllFileCompletionOptionsPresent() {
        Map<String, String> m = base();
        m.put("file-completion.flag-column", "is_eof");
        m.put("file-completion.message-column", "kafka_msg");
        m.put("file-completion.kafka.topic", "events");
        m.put("file-completion.kafka.properties.bootstrap.servers", "localhost:9092");
        OceanBaseConnectorOptions opts = new OceanBaseConnectorOptions(m);
        assertDoesNotThrow(opts::validateFileCompletionOptions);
        assertFalse(opts.isFileCompletionKafkaEnabled());
    }

    @Test
    void kafkaPropertyPrefixIsForwardedToProducerProperties() {
        Map<String, String> m = base();
        m.put("file-completion.kafka.notification-enabled", "true");
        m.put("file-completion.flag-column", "is_eof");
        m.put("file-completion.message-column", "kafka_msg");
        m.put("file-completion.kafka.topic", "events");
        m.put("file-completion.kafka.properties.bootstrap.servers", "h1:9092,h2:9092");
        m.put("file-completion.kafka.properties.security.protocol", "PLAINTEXT");
        OceanBaseConnectorOptions opts = new OceanBaseConnectorOptions(m);
        assertDoesNotThrow(opts::validateFileCompletionOptions);
        Properties props = opts.buildFileCompletionKafkaProducerProperties();
        assertTrue(props.getProperty("bootstrap.servers").contains("9092"));
        assertEquals("PLAINTEXT", props.getProperty("security.protocol"));
    }
}
