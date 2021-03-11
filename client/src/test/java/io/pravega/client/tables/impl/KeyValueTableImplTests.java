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

import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableConfiguration;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the {@link KeyValueTableImpl} class. This uses mocked {@link TableSegment}s so it does not actually
 * verify over-the-wire commands. Integration tests (`io.pravega.test.integration.KeyValueTableImplTests`) cover end-to-end
 * scenarios instead.
 */
public class KeyValueTableImplTests extends KeyValueTableTestBase {
    protected static final KeyValueTableInfo KVT = new KeyValueTableInfo("Scope", "KVT");
    protected MockController controller;
    protected MockTableSegmentFactory segmentFactory;
    private MockConnectionFactoryImpl connectionFactory;
    private KeyValueTable<Long, String> keyValueTable;

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Override
    protected KeyValueTable<Long, String> createKeyValueTable() {
        return this.keyValueTable;
    }

    @Override
    protected <K, V> KeyValueTable<K, V> createKeyValueTable(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        return new KeyValueTableImpl<>(KVT, this.segmentFactory, this.controller, keySerializer, valueSerializer);
    }

    @Before
    public void setup() throws Exception {
        super.setup();
        this.connectionFactory = new MockConnectionFactoryImpl();
        this.controller = new MockController("localhost", 0, this.connectionFactory, false);
        boolean isScopeCreated = this.controller.createScope(KVT.getScope()).get().booleanValue();
        Assert.assertTrue(isScopeCreated);
        createKeyValueTableInController();
        this.segmentFactory = new MockTableSegmentFactory(getSegmentCount(), executorService());
        this.keyValueTable = createKeyValueTable(KEY_SERIALIZER, VALUE_SERIALIZER);
    }

    @After
    public void tearDown() {
        this.keyValueTable.close();
        this.controller.close();
        this.connectionFactory.close();
    }

    protected void createKeyValueTableInController() {
        this.controller.createKeyValueTable(KVT.getScope(), KVT.getKeyValueTableName(),
                KeyValueTableConfiguration.builder().partitionCount(getSegmentCount()).build());
    }

    /**
     * Tests the {@link KeyValueTable#close()} method.
     */
    @Test
    public void testClose() {
        @Cleanup
        val kvt = createKeyValueTable();
        val iteration = new AtomicInteger(0);
        forEveryKeyFamily(true, (keyFamily, keyIds) -> {
            for (val k : keyIds) {
                kvt.putIfAbsent(keyFamily, getKey(k), getValue(0, iteration.get())).join();
            }
        });

        Assert.assertEquals("Unexpected number of open segments before closing.", getSegmentCount(), this.segmentFactory.getOpenSegmentCount());
        kvt.close();
        Assert.assertEquals("Not expecting any open segments after closing.", 0, this.segmentFactory.getOpenSegmentCount());
    }
}
