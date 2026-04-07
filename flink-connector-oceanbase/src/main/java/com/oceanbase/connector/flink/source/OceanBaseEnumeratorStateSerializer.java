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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Serializer for {@link OceanBaseEnumeratorState}. */
public class OceanBaseEnumeratorStateSerializer
        implements SimpleVersionedSerializer<OceanBaseEnumeratorState> {

    private static final int CURRENT_VERSION = 3;
    private final OceanBaseSplitSerializer splitSerializer = new OceanBaseSplitSerializer();

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(OceanBaseEnumeratorState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            writeSplitList(out, state.getInFlightSplits());
            writeSplitList(out, state.getPendingSplits());

            return baos.toByteArray();
        }
    }

    @Override
    public OceanBaseEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {

            // State version and split serializer version are kept in sync
            int splitVersion = version;
            List<OceanBaseSplit> inFlightSplits = readSplitList(in, splitVersion);
            List<OceanBaseSplit> pendingSplits = readSplitList(in, splitVersion);

            return new OceanBaseEnumeratorState(inFlightSplits, pendingSplits);
        }
    }

    private void writeSplitList(DataOutputStream out, List<OceanBaseSplit> splits)
            throws IOException {
        out.writeInt(splits.size());
        for (OceanBaseSplit split : splits) {
            byte[] splitBytes = splitSerializer.serialize(split);
            out.writeInt(splitBytes.length);
            out.write(splitBytes);
        }
    }

    private List<OceanBaseSplit> readSplitList(DataInputStream in, int splitVersion)
            throws IOException {
        int size = in.readInt();
        List<OceanBaseSplit> splits = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            int length = in.readInt();
            byte[] splitBytes = new byte[length];
            in.readFully(splitBytes);
            splits.add(splitSerializer.deserialize(splitVersion, splitBytes));
        }
        return splits;
    }
}
