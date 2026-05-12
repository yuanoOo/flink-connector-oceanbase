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

package com.oceanbase.connector.flink.sink;

import com.oceanbase.connector.flink.OceanBaseConnectorOptions;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.function.Function;

/**
 * Kafka producer used to emit one record per OSS file completion row after OB flush. Lazily
 * initialized on first use; driven by the single sink-writer thread, so no synchronization needed.
 */
public class KafkaFileCompletionNotifier implements FileCompletionNotifier {

    private static final long serialVersionUID = 1L;

    /**
     * Test hook: when non-null, used instead of {@link KafkaProducer} to create the underlying
     * producer. Set this from tests to inject a {@link
     * org.apache.kafka.clients.producer.MockProducer} and unset it in a {@code finally} block. Not
     * for production use.
     */
    static volatile Function<Properties, Producer<String, String>> producerFactoryForTest;

    private final Properties kafkaProps;
    private final String topic;

    private transient Producer<String, String> producer;

    public KafkaFileCompletionNotifier(OceanBaseConnectorOptions options) {
        this.kafkaProps = options.buildFileCompletionKafkaProducerProperties();
        this.topic = options.getFileCompletionKafkaTopic();
    }

    @Override
    public void notify(String message) throws Exception {
        if (producer == null) {
            Function<Properties, Producer<String, String>> factory = producerFactoryForTest;
            producer =
                    factory != null ? factory.apply(kafkaProps) : new KafkaProducer<>(kafkaProps);
        }
        String value = message == null ? "" : message;
        producer.send(new ProducerRecord<>(topic, null, value)).get();
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
            producer = null;
        }
    }
}
