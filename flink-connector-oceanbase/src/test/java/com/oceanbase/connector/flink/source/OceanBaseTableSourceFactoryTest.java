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

import com.oceanbase.connector.flink.ConnectorOptions;

import org.apache.flink.configuration.ConfigOption;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link OceanBaseTableSourceFactory}. */
public class OceanBaseTableSourceFactoryTest {

    private final OceanBaseTableSourceFactory factory = new OceanBaseTableSourceFactory();

    @Test
    public void testFactoryIdentifier() {
        assertEquals("oceanbase", factory.factoryIdentifier());
    }

    @Test
    public void testRequiredOptions() {
        Set<ConfigOption<?>> required = factory.requiredOptions();
        assertTrue(required.contains(ConnectorOptions.URL));
        assertTrue(required.contains(ConnectorOptions.USERNAME));
        assertTrue(required.contains(ConnectorOptions.PASSWORD));
        assertTrue(required.contains(ConnectorOptions.SCHEMA_NAME));
        assertTrue(required.contains(ConnectorOptions.TABLE_NAME));
        assertEquals(5, required.size());
    }

    @Test
    public void testOptionalOptionsContainSourceOptions() {
        Set<ConfigOption<?>> optional = factory.optionalOptions();
        assertTrue(optional.contains(OceanBaseTableSourceFactory.COMPATIBLE_MODE));
        assertTrue(optional.contains(OceanBaseTableSourceFactory.SPLIT_SIZE));
        assertTrue(optional.contains(OceanBaseTableSourceFactory.CHUNK_KEY_COLUMN));
        assertTrue(optional.contains(OceanBaseTableSourceFactory.FETCH_SIZE));
    }

    @Test
    public void testDefaultValues() {
        assertEquals(8192, OceanBaseTableSourceFactory.SPLIT_SIZE.defaultValue().intValue());
        assertEquals("MySQL", OceanBaseTableSourceFactory.COMPATIBLE_MODE.defaultValue());
        assertEquals(1024, OceanBaseTableSourceFactory.FETCH_SIZE.defaultValue().intValue());
    }
}
