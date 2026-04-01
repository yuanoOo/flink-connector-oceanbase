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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** State for OceanBase split enumerator. */
public class OceanBaseEnumeratorState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<OceanBaseSplit> assignedSplits;
    private final List<OceanBaseSplit> pendingSplits;

    public OceanBaseEnumeratorState() {
        this.assignedSplits = new ArrayList<>();
        this.pendingSplits = new ArrayList<>();
    }

    public OceanBaseEnumeratorState(
            List<OceanBaseSplit> assignedSplits, List<OceanBaseSplit> pendingSplits) {
        this.assignedSplits = assignedSplits;
        this.pendingSplits = pendingSplits;
    }

    public List<OceanBaseSplit> getAssignedSplits() {
        return assignedSplits;
    }

    public List<OceanBaseSplit> getPendingSplits() {
        return pendingSplits;
    }
}
