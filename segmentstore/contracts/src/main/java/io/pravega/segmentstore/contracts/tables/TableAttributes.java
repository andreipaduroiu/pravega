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

import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentType;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Defines Table Segment-specific Core Attributes.
 */
public class TableAttributes extends Attributes {
    /**
     * Defines an attribute that is used to store the first offset of a (Table) Segment that has not yet been indexed.
     */
    public static final AttributeId INDEX_OFFSET = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, TABLE_ATTRIBUTES_START_OFFSET);

    /**
     * Defines an attribute that is used to store the number of indexed Table Entries in a (Table) Segment.
     */
    public static final AttributeId ENTRY_COUNT = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, TABLE_ATTRIBUTES_START_OFFSET + 1);

    /**
     * Defines an attribute that is used to store the number of Table Buckets in a (Table) Segment.
     */
    public static final AttributeId BUCKET_COUNT = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, TABLE_ATTRIBUTES_START_OFFSET + 2);

    /**
     * Defines an attribute that is used to store number of entries (active and overwritten) in a (Table) Segment.
     */
    public static final AttributeId TOTAL_ENTRY_COUNT = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, TABLE_ATTRIBUTES_START_OFFSET + 3);

    /**
     * Defines an attribute that is used to store the offset of a (Table) Segment where compaction has last run at.
     */
    public static final AttributeId COMPACTION_OFFSET = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, TABLE_ATTRIBUTES_START_OFFSET + 4);

    /**
     * Defines an attribute that is used to set the minimum utilization (as a percentage of {@link #ENTRY_COUNT} out of
     * {@link #TOTAL_ENTRY_COUNT}) of a Table Segment below which a Table Compaction is triggered.
     */
    public static final AttributeId MIN_UTILIZATION = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, TABLE_ATTRIBUTES_START_OFFSET + 5);

    /**
     * Defines an attribute that is used to indicate whether the Table Segment is Sorted (by Key) or not. This value
     * cannot be changed after the Table Segment is created.
     * TODO: deprecate in favor of {@link SegmentType#isSortedTableSegment()} (https://github.com/pravega/pravega/issues/5267).
     */
    public static final AttributeId SORTED = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, TABLE_ATTRIBUTES_START_OFFSET + 6);

    /**
     * Defines a Map that contains all Table Attributes along with their default values.
     */
    public static final Map<AttributeId, Long> DEFAULT_VALUES = Collections.unmodifiableMap(
            Arrays.stream(TableAttributes.class.getDeclaredFields())
                    .filter(f -> f.getType().equals(AttributeId.class))
                    .collect(Collectors.toMap(f -> {
                        try {
                            return (AttributeId) f.get(null);
                        } catch (IllegalAccessException ex) {
                            throw new RuntimeException(ex);
                        }
                    }, f -> 0L)));
}
