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
import java.nio.charset.StandardCharsets;

/** Serializer for {@link OceanBaseSplit}. */
public class OceanBaseSplitSerializer implements SimpleVersionedSerializer<OceanBaseSplit> {

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(OceanBaseSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            writeString(out, split.splitId());
            writeString(out, split.getSchemaName());
            writeString(out, split.getTableName());
            writeString(out, split.getSplitColumn());
            writeObject(out, split.getSplitStart());
            writeObject(out, split.getSplitEnd());

            return baos.toByteArray();
        }
    }

    @Override
    public OceanBaseSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {

            String splitId = readString(in);
            String schemaName = readString(in);
            String tableName = readString(in);
            String splitColumn = readString(in);
            Object splitStart = readObject(in);
            Object splitEnd = readObject(in);

            return new OceanBaseSplit(
                    splitId, schemaName, tableName, splitColumn, splitStart, splitEnd);
        }
    }

    private void writeString(DataOutputStream out, String value) throws IOException {
        if (value == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    private String readString(DataInputStream in) throws IOException {
        if (in.readBoolean()) {
            int length = in.readInt();
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return null;
    }

    private void writeObject(DataOutputStream out, Object value) throws IOException {
        if (value == null) {
            out.writeByte(0);
        } else if (value instanceof String) {
            out.writeByte(1);
            writeString(out, (String) value);
        } else if (value instanceof Long) {
            out.writeByte(2);
            out.writeLong((Long) value);
        } else if (value instanceof Integer) {
            out.writeByte(3);
            out.writeInt((Integer) value);
        } else if (value instanceof Double) {
            out.writeByte(4);
            out.writeDouble((Double) value);
        } else if (value instanceof java.math.BigDecimal) {
            out.writeByte(5);
            writeString(out, value.toString());
        } else {
            out.writeByte(6);
            writeString(out, value.toString());
        }
    }

    private Object readObject(DataInputStream in) throws IOException {
        byte type = in.readByte();
        switch (type) {
            case 0:
                return null;
            case 1:
                return readString(in);
            case 2:
                return in.readLong();
            case 3:
                return in.readInt();
            case 4:
                return in.readDouble();
            case 5:
                return new java.math.BigDecimal(readString(in));
            case 6:
                return readString(in);
            default:
                throw new IOException("Unknown object type: " + type);
        }
    }
}
