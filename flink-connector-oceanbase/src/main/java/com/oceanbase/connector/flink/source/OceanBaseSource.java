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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;

/** OceanBase parallel snapshot source. */
public class OceanBaseSource
        implements Source<RowData, OceanBaseSplit, OceanBaseEnumeratorState>,
                ResultTypeQueryable<RowData> {

    private static final long serialVersionUID = 1L;

    private final OceanBaseSourceConfig config;
    private final DataType producedDataType;

    public OceanBaseSource(OceanBaseSourceConfig config, DataType producedDataType) {
        this.config = config;
        this.producedDataType = producedDataType;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, OceanBaseSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        return new OceanBaseSourceReader(readerContext, config, producedDataType);
    }

    @Override
    public SplitEnumerator<OceanBaseSplit, OceanBaseEnumeratorState> createEnumerator(
            SplitEnumeratorContext<OceanBaseSplit> enumContext) throws Exception {
        return new OceanBaseSplitEnumerator(enumContext, config, null);
    }

    @Override
    public SplitEnumerator<OceanBaseSplit, OceanBaseEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<OceanBaseSplit> enumContext, OceanBaseEnumeratorState checkpoint)
            throws Exception {
        return new OceanBaseSplitEnumerator(enumContext, config, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<OceanBaseSplit> getSplitSerializer() {
        return new OceanBaseSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<OceanBaseEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new OceanBaseEnumeratorStateSerializer();
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        if (producedDataType != null) {
            TypeInformation<?> typeInfo =
                    TypeConversions.fromDataTypeToLegacyInfo(producedDataType);
            @SuppressWarnings("unchecked")
            TypeInformation<RowData> result = (TypeInformation<RowData>) typeInfo;
            return result;
        }
        throw new IllegalArgumentException("producedDataType must not be null");
    }
}
