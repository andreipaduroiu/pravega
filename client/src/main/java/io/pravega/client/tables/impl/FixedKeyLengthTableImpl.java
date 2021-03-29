/**
 * Copyright Pravega Authors.
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
package io.pravega.client.tables.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.Serializer;
import io.pravega.client.tables.BadKeyVersionException;
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.IteratorState;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableMap;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.TableKey;
import io.pravega.client.tables.Version;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public class FixedKeyLengthTableImpl<KeyT, ValueT> implements KeyValueTable<KeyT, ValueT>, AutoCloseable {
    //region Members

    private final KeyValueTableInfo kvt;
    private final Serializer<KeyT> keySerializer;
    private final Serializer<ValueT> valueSerializer;
    private final SegmentSelector selector;
    private final String logTraceId;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link KeyValueTableImpl} class.
     *
     * @param kvt                 A {@link KeyValueTableInfo} containing information about the Key-Value Table.
     * @param tableSegmentFactory Factory to create {@link TableSegment} instances.
     * @param controller          Controller client.
     * @param keySerializer       Serializer for keys.
     * @param valueSerializer     Serializer for values.
     */
    FixedKeyLengthTableImpl(@NonNull KeyValueTableInfo kvt, @NonNull TableSegmentFactory tableSegmentFactory, @NonNull Controller controller,
                            @NonNull Serializer<KeyT> keySerializer, @NonNull Serializer<ValueT> valueSerializer) {
        this.kvt = kvt;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.selector = new SegmentSelector(this.kvt, controller, tableSegmentFactory);
        this.logTraceId = String.format("KeyValueTableFixed[%s]", this.kvt.getScopedName());
        this.closed = new AtomicBoolean(false);
        log.info("{}: Initialized. SegmentCount={}.", this.logTraceId, this.selector.getSegmentCount());
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            this.selector.close();
            log.info("{}: Closed.", this.logTraceId);
        }
    }

    //endregion

    //region KeyValueTable Implementation

    @Override
    public CompletableFuture<Version> put(@Nullable String keyFamily, @NonNull KeyT key, @NonNull ValueT value) {
        Preconditions.checkArgument(keyFamily == null, "keyFamily not supported.");
        ByteBuf keySerialization = serializeKey(key);
        TableSegment s = this.selector.getTableSegment(null, keySerialization);
        return updateToSegment(s, toTableSegmentEntry(keySerialization, serializeValue(value), Version.NO_VERSION));
    }

    @Override
    public CompletableFuture<Version> putIfAbsent(@Nullable String keyFamily, @NonNull KeyT key, @NonNull ValueT value) {
        Preconditions.checkArgument(keyFamily == null, "keyFamily not supported.");
        ByteBuf keySerialization = serializeKey(key);
        TableSegment s = this.selector.getTableSegment(null, keySerialization);
        return updateToSegment(s, toTableSegmentEntry(keySerialization, serializeValue(value), Version.NOT_EXISTS));
    }

    @Override
    public CompletableFuture<List<Version>> putAll(@NonNull String keyFamily, @NonNull Iterable<Map.Entry<KeyT, ValueT>> entries) {
        throw new UnsupportedOperationException("putAll");
    }

    @Override
    public CompletableFuture<Version> replace(@Nullable String keyFamily, @NonNull KeyT key, @NonNull ValueT value, @NonNull Version version) {
        Preconditions.checkArgument(keyFamily == null, "keyFamily not supported.");
        ByteBuf keySerialization = serializeKey(key);
        TableSegment s = this.selector.getTableSegment(null, keySerialization);
        validateKeyVersionSegment(s, version);
        return updateToSegment(s, toTableSegmentEntry(keySerialization, serializeValue(value), version));
    }

    @Override
    public CompletableFuture<List<Version>> replaceAll(@NonNull String keyFamily, @NonNull Iterable<TableEntry<KeyT, ValueT>> tableEntries) {
        throw new UnsupportedOperationException("replaceAll");
    }

    @Override
    public CompletableFuture<Void> remove(@Nullable String keyFamily, @NonNull KeyT key) {
        Preconditions.checkArgument(keyFamily == null, "keyFamily not supported.");
        ByteBuf keySerialization = serializeKey(key);
        TableSegment s = this.selector.getTableSegment(null, keySerialization);
        return removeFromSegment(s, Iterators.singletonIterator(toTableSegmentKey(keySerialization, Version.NO_VERSION)));
    }

    @Override
    public CompletableFuture<Void> remove(@Nullable String keyFamily, @NonNull KeyT key, @NonNull Version version) {
        Preconditions.checkArgument(keyFamily == null, "keyFamily not supported.");
        ByteBuf keySerialization = serializeKey(key);
        TableSegment s = this.selector.getTableSegment(null, keySerialization);
        validateKeyVersionSegment(s, version);
        return removeFromSegment(s, Iterators.singletonIterator(toTableSegmentKey(keySerialization, version)));
    }

    @Override
    public CompletableFuture<Void> removeAll(@Nullable String keyFamily, @NonNull Iterable<TableKey<KeyT>> tableKeys) {
        throw new UnsupportedOperationException("removeAll");
    }

    @Override
    public CompletableFuture<TableEntry<KeyT, ValueT>> get(@Nullable String keyFamily, @NonNull KeyT key) {
        Preconditions.checkArgument(keyFamily == null, "keyFamily not supported.");
        return getAll(null, Collections.singleton(key))
                .thenApply(r -> r.get(0));
    }

    @Override
    public CompletableFuture<List<TableEntry<KeyT, ValueT>>> getAll(@Nullable String keyFamily, @NonNull Iterable<KeyT> keys) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkArgument(keyFamily == null, "keyFamily not supported.");
        Iterator<ByteBuf> serializedKeys = StreamSupport.stream(keys.spliterator(), false)
                .map(this::serializeKey)
                .iterator();
        // We are dealing with multiple segments.
        return getFromMultiSegments(serializedKeys);
    }

    @Override
    public AsyncIterator<IteratorItem<TableKey<KeyT>>> keyIterator(@NonNull String keyFamily, int maxKeysAtOnce, @Nullable IteratorState state) {
        throw new UnsupportedOperationException("keyIterator");
    }

    @Override
    public AsyncIterator<IteratorItem<TableEntry<KeyT, ValueT>>> entryIterator(@NonNull String keyFamily, int maxEntriesAtOnce, @Nullable IteratorState state) {
        throw new UnsupportedOperationException("entryIterator");
    }

    @Override
    public KeyValueTableMap<KeyT, ValueT> getMapFor(@Nullable String keyFamily) {
        throw new UnsupportedOperationException("getMapFor");
    }

    //endregion

    //region Helpers

    private CompletableFuture<Version> updateToSegment(TableSegment segment, TableSegmentEntry tableSegmentEntry) {
        return updateToSegment(segment, Iterators.singletonIterator(tableSegmentEntry)).thenApply(r -> r.get(0));
    }

    private CompletableFuture<List<Version>> updateToSegment(TableSegment segment, Iterator<TableSegmentEntry> tableSegmentEntries) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return segment.put(tableSegmentEntries)
                .thenApply(versions -> versions.stream().map(v -> new VersionImpl(segment.getSegmentId(), v)).collect(Collectors.toList()));
    }

    private CompletableFuture<Void> removeFromSegment(TableSegment segment, Iterator<TableSegmentKey> tableSegmentKeys) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return segment.remove(tableSegmentKeys);
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<List<TableEntry<KeyT, ValueT>>> getFromMultiSegments(Iterator<ByteBuf> serializedKeys) {
        val bySegment = new HashMap<TableSegment, KeyGroup>();
        val count = new AtomicInteger(0);
        serializedKeys.forEachRemaining(k -> {
            TableSegment ts = this.selector.getTableSegment(null, k);
            KeyGroup g = bySegment.computeIfAbsent(ts, t -> new KeyGroup());
            g.add(k, count.getAndIncrement());
        });

        val futures = new HashMap<TableSegment, CompletableFuture<List<TableSegmentEntry>>>();
        bySegment.forEach((ts, kg) -> futures.put(ts, ts.get(kg.keys.iterator())));
        return Futures.allOf(futures.values())
                .thenApply(v -> {
                    val r = new TableEntry[count.get()];
                    futures.forEach((ts, f) -> {
                        KeyGroup kg = bySegment.get(ts);
                        assert f.isDone() : "incomplete CompletableFuture returned by Futures.allOf";
                        val segmentResult = f.join();
                        assert segmentResult.size() == kg.ordinals.size() : "segmentResult count mismatch";
                        for (int i = 0; i < kg.ordinals.size(); i++) {
                            assert r[kg.ordinals.get(i)] == null : "overlapping ordinals";
                            r[kg.ordinals.get(i)] = fromTableSegmentEntry(ts, segmentResult.get(i));
                        }
                    });
                    return Arrays.asList(r);
                });
    }

    private TableSegmentKey toTableSegmentKey(ByteBuf key, Version keyVersion) {
        return new TableSegmentKey(key, toTableSegmentVersion(keyVersion));
    }

    private TableSegmentEntry toTableSegmentEntry(ByteBuf keySerialization, ByteBuf valueSerialization, Version keyVersion) {
        return new TableSegmentEntry(toTableSegmentKey(keySerialization, keyVersion), valueSerialization);
    }

    private TableSegmentKeyVersion toTableSegmentVersion(Version version) {
        return version == null ? TableSegmentKeyVersion.NO_VERSION : TableSegmentKeyVersion.from(version.asImpl().getSegmentVersion());
    }

    private TableEntry<KeyT, ValueT> fromTableSegmentEntry(TableSegment s, TableSegmentEntry e) {
        if (e == null) {
            return null;
        }

        TableKey<KeyT> segmentKey = fromTableSegmentKey(s, e.getKey());
        ValueT value = deserializeValue(e.getValue());
        return TableEntry.versioned(segmentKey.getKey(), segmentKey.getVersion(), value);
    }

    private TableKey<KeyT> fromTableSegmentKey(TableSegment s, TableSegmentKey tableSegmentKey) {
        KeyT key = deserializeKey(tableSegmentKey.getKey());
        Version version = new VersionImpl(s.getSegmentId(), tableSegmentKey.getVersion());
        return TableKey.versioned(key, version);
    }

    private ByteBuf serializeKey(KeyT k) {
        return Unpooled.wrappedBuffer(this.keySerializer.serialize(k));
    }

    private KeyT deserializeKey(ByteBuf keySerialization) {
        KeyT key = this.keySerializer.deserialize(keySerialization.nioBuffer());
        keySerialization.release();
        return key;
    }

    private ByteBuf serializeValue(ValueT v) {
        ByteBuf valueSerialization = Unpooled.wrappedBuffer(this.valueSerializer.serialize(v));
        Preconditions.checkArgument(valueSerialization.readableBytes() <= KeyValueTable.MAXIMUM_SERIALIZED_VALUE_LENGTH,
                "Value Too Long. Expected at most %s, actual %s.", KeyValueTable.MAXIMUM_SERIALIZED_VALUE_LENGTH, valueSerialization.readableBytes());
        return valueSerialization;
    }

    private ValueT deserializeValue(ByteBuf s) {
        ValueT result = this.valueSerializer.deserialize(s.nioBuffer());
        s.release();
        return result;
    }

    @SneakyThrows(BadKeyVersionException.class)
    private void validateKeyVersionSegment(TableSegment ts, Version version) {
        if (version == null) {
            return;
        }

        VersionImpl impl = version.asImpl();
        boolean valid = impl.getSegmentId() == VersionImpl.NO_SEGMENT_ID || ts.getSegmentId() == impl.getSegmentId();
        if (!valid) {
            throw new BadKeyVersionException(this.kvt.getScopedName(), "Wrong TableSegment.");
        }
    }

    //endregion

    //region Helper classes

    private static class KeyGroup {
        final ArrayList<ByteBuf> keys = new ArrayList<>();
        final ArrayList<Integer> ordinals = new ArrayList<>();

        void add(ByteBuf key, int ordinal) {
            this.keys.add(key);
            this.ordinals.add(ordinal);
        }
    }

    //endregion
}
