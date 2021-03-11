/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables.impl;

import io.pravega.client.stream.Serializer;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableConfiguration;
import org.junit.Ignore;
import org.junit.Test;

public class FixedKeyValueTableImplTests extends KeyValueTableImplTests {
    private static final int KEY_LENGTH = 32;

    @Override
    protected void createKeyValueTableInController() {
        this.controller.createKeyValueTable(KVT.getScope(), KVT.getKeyValueTableName(),
                KeyValueTableConfiguration.builder().partitionCount(getSegmentCount()).keyLength(KEY_LENGTH).build());
    }

    @Override
    protected <K, V> KeyValueTable<K, V> createKeyValueTable(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        return new FixedKeyLengthTableImpl<>(KVT, this.segmentFactory, this.controller, keySerializer, valueSerializer);
    }

    @Override
    protected int getKeyFamilyCount() {
        return 0; // Not supported in this prototype.
    }

    @Override
    protected int getKeysWithoutKeyFamily() {
        return Math.min(TableSegment.MAXIMUM_BATCH_KEY_COUNT, getKeysPerKeyFamily());
    }

    @Test
    @Ignore
    public void testLargeBatchUpdates() {
        super.testLargeBatchUpdates();
    }

    @Test
    @Ignore
    public void testLargeEntryBatchRetrieval() {
        super.testLargeEntryBatchRetrieval();
    }

    @Test
    @Ignore
    public void testLargeKeyValueUpdates() {
        super.testLargeKeyValueUpdates();
    }

    @Test
    @Ignore
    public void testIterators() {
        super.testIterators();
    }

    @Test
    @Ignore
    public void testMultiKeyOperations() {
        super.testMultiKeyOperations();
    }
}
