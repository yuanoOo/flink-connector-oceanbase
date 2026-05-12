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

import java.io.Serializable;

/** Sends a file-completion notification after OceanBase flush succeeds. */
public interface FileCompletionNotifier extends AutoCloseable, Serializable {

    FileCompletionNotifier NO_OP = message -> {};

    static FileCompletionNotifier noop() {
        return NO_OP;
    }

    /**
     * Invoked after the corresponding {@link
     * com.oceanbase.connector.flink.table.FileCompletionDataChangeRecord} and all prior buffered
     * rows have been flushed to OceanBase successfully.
     */
    void notify(String message) throws Exception;

    @Override
    default void close() throws Exception {}
}
