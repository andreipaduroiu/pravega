/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import java.util.Collection;
import lombok.Builder;
import lombok.Data;

/**
 * Segment Append.
 */
@Data
@Builder
public class SegmentAppend {
    private final BufferView data;
    @Builder.Default
    private final long offset = -1;
    private final Collection<AttributeUpdate> attributeUpdates;
    @Builder.Default
    private final boolean variableAttributeIds = false;

    public boolean isOffsetConditional() {
        return this.offset >= 0;
    }

    @Override
    public String toString() {
        return String.format("Length = %s, Offset = %s, Attributes = %s (Variable = %s)",
                this.data.getLength(), isOffsetConditional() ? "(none)" : this.offset,
                this.attributeUpdates == null ? 0 : this.attributeUpdates.size(), this.variableAttributeIds);
    }
}
