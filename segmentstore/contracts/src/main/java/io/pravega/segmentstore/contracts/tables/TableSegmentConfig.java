/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts.tables;

import lombok.Builder;
import lombok.Getter;

/**
 * Configuration for a Table Segment.
 */
@Builder
@Getter
public class TableSegmentConfig {
    public static final TableSegmentConfig NO_CONFIG = TableSegmentConfig.builder().build();
    @Builder.Default
    private final int keyLength = 0;

    @Override
    public String toString() {
        return String.format("KeyLength = %s", this.keyLength);
    }
}
