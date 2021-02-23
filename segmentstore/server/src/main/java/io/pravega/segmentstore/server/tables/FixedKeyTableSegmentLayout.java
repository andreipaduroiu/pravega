/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import com.google.common.base.Preconditions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.IllegalDataFormatException;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateByReference;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.IteratorState;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableSegmentConfig;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.WriterSegmentProcessor;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

class FixedKeyTableSegmentLayout extends TableSegmentLayout {
    //region Constructor

    FixedKeyTableSegmentLayout(SegmentContainer segmentContainer, Config config, ScheduledExecutorService executor) {
        super(segmentContainer, config, executor);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {

    }

    //endregion

    //region TableSegmentLayout Implementation

    @Override
    Collection<WriterSegmentProcessor> createWriterSegmentProcessors(UpdateableSegmentMetadata metadata) {
        return Collections.emptyList(); // TODO I don't think we need anything, except maybe for compaction?
    }

    @Override
    void setNewSegmentAttributes(@NonNull SegmentType segmentType, @NonNull TableSegmentConfig config, @NonNull Map<AttributeId, Long> attributes) {
        ensureSegmentType("", segmentType);
        ensureValidKeyLength("", config.getKeyLength());
        Preconditions.checkArgument(config.getKeyLength() % Long.BYTES == 0, "KeyLength must be a multiple of %s; given %s.", Long.BYTES, config.getKeyLength());
        attributes.put(Attributes.ATTRIBUTE_ID_LENGTH, (long) config.getKeyLength());
    }

    @Override
    CompletableFuture<Void> deleteSegment(@NonNull String segmentName, boolean mustBeEmpty, Duration timeout) {
        Preconditions.checkArgument(!mustBeEmpty, "mustBeEmpty not supported on Fixed-Key Table Segments.");
        return this.segmentContainer.deleteStreamSegment(segmentName, timeout);
    }

    @Override
    CompletableFuture<List<Long>> put(@NonNull DirectSegmentAccess segment, @NonNull List<TableEntry> entries, long tableSegmentOffset, TimeoutTimer timer) {
        val segmentInfo = segment.getInfo();
        ensureSegmentType(segmentInfo.getName(), segmentInfo.getType());
        val segmentKeyLength = getSegmentKeyLength(segmentInfo);
        ensureValidKeyLength(segmentInfo.getName(), segmentKeyLength);

        val attributeUpdates = new ArrayList<AttributeUpdate>(entries.size());
        int batchOffset = 0;
        val batchOffsets = new ArrayList<Integer>();
        for (val e : entries) {
            val key = e.getKey();
            Preconditions.checkArgument(key.getKey().getLength() == segmentKeyLength,
                    "Entry Key Length for key `%s` incompatible with segment '%s' which requires key lengths of %s.",
                    key, segmentInfo.getName(), segmentKeyLength);

            val attributeId = AttributeId.from(key.getKey().getCopy());
            val ref = new AttributeUpdateByReference.SegmentLengthReference(batchOffset);
            val au = key.hasVersion()
                    ? new AttributeUpdateByReference(attributeId, AttributeUpdateType.ReplaceIfEquals, ref, key.getVersion())
                    : new AttributeUpdateByReference(attributeId, AttributeUpdateType.Replace, ref);
            attributeUpdates.add(au);
            batchOffsets.add(batchOffset);
            batchOffset += this.serializer.getUpdateLength(e);
        }

        val serializedEntries = this.serializer.serializeUpdate(entries);
        val result = tableSegmentOffset == NO_OFFSET
                ? segment.append(serializedEntries, attributeUpdates, timer.getRemaining()) // TODO Append2
                : segment.append(serializedEntries, attributeUpdates, tableSegmentOffset, timer.getRemaining()); // TODO Append2
        return result.thenApply(segmentOffset -> batchOffsets.stream().map(offset -> offset + segmentOffset).collect(Collectors.toList()));
    }

    @Override
    CompletableFuture<Void> remove(@NonNull DirectSegmentAccess segment, @NonNull Collection<TableKey> keys, long tableSegmentOffset, TimeoutTimer timer) {
        val segmentInfo = segment.getInfo();
        ensureSegmentType(segmentInfo.getName(), segmentInfo.getType());
        val segmentKeyLength = getSegmentKeyLength(segmentInfo);
        ensureValidKeyLength(segmentInfo.getName(), segmentKeyLength);

        val attributeUpdates = new ArrayList<AttributeUpdate>(keys.size());
        for (val key : keys) {
            Preconditions.checkArgument(key.getKey().getLength() == segmentKeyLength,
                    "Key Length for key `%s` incompatible with segment '%s' which requires key lengths of %s.",
                    key, segmentInfo.getName(), segmentKeyLength);

            val attributeId = AttributeId.from(key.getKey().getCopy());
            val au = key.hasVersion()
                    ? new AttributeUpdate(attributeId, AttributeUpdateType.ReplaceIfEquals, Attributes.NULL_ATTRIBUTE_VALUE, key.getVersion())
                    : new AttributeUpdate(attributeId, AttributeUpdateType.Replace, Attributes.NULL_ATTRIBUTE_VALUE);
            attributeUpdates.add(au);
        }

        val result = tableSegmentOffset == NO_OFFSET
                ? segment.updateAttributes(attributeUpdates, timer.getRemaining())
                : segment.append(BufferView.empty(), attributeUpdates, tableSegmentOffset, timer.getRemaining()); // TODO Append2
        return Futures.toVoid(result);
    }

    @Override
    CompletableFuture<List<TableEntry>> get(@NonNull DirectSegmentAccess segment, @NonNull List<BufferView> keys, TimeoutTimer timer) {
        val segmentInfo = segment.getInfo();
        ensureSegmentType(segmentInfo.getName(), segmentInfo.getType());
        val attributes = keys.stream().map(key -> {
            ensureValidKeyLength(segmentInfo.getName(), key.getLength());
            return AttributeId.from(key.getCopy());
        }).collect(Collectors.toList());

        return segment.getAttributes(attributes, false, timer.getRemaining())
                .thenComposeAsync(attributeValues -> {
                    val result = new ArrayList<CompletableFuture<TableEntry>>(attributes.size());
                    for (val attributeId : attributes) {
                        val segmentOffset = attributeValues.getOrDefault(attributeId, NO_OFFSET);
                        if (segmentOffset < 0) {
                            result.add(CompletableFuture.completedFuture(null));
                        } else {
                            // TODO: whenever we do compaction, account for StreamSegmentTruncatedException.
                            val entryReader = AsyncTableEntryReader.readEntry(attributeId.toBuffer(), segmentOffset, this.serializer, timer);
                            val readResult = segment.read(segmentOffset, EntrySerializer.MAX_SERIALIZATION_LENGTH, timer.getRemaining());
                            AsyncReadResultProcessor.process(readResult, entryReader, this.executor);
                            result.add(entryReader.getResult());
                        }
                    }
                    return Futures.allOfWithResults(result);
                }, this.executor);
    }

    @Override
    CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(@NonNull DirectSegmentAccess segment, IteratorArgs args) {
        return newIterator(segment, this::getIteratorKeys, args);
    }

    @Override
    CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(@NonNull DirectSegmentAccess segment, IteratorArgs args) {
        return newIterator(segment, this::getIteratorEntries, args);
    }

    @Override
    AsyncIterator<IteratorItem<TableEntry>> entryDeltaIterator(@NonNull DirectSegmentAccess segment, long fromPosition, Duration fetchTimeout) {
        throw new UnsupportedOperationException("entryDeltaIterator");
    }

    //endregion

    //region Helpers

    private <T> CompletableFuture<AsyncIterator<IteratorItem<T>>> newIterator(@NonNull DirectSegmentAccess segment,
                                                                              @NonNull GetIteratorItem<T> getItems,
                                                                              @NonNull IteratorArgs args) {
        Preconditions.checkArgument(args.getPrefixFilter() == null, "PrefixFilter not supported.");
        IteratorStateImpl state;
        try {
            state = args.getSerializedState() == null ? null : IteratorStateImpl.deserialize(args.getSerializedState());
        } catch (IOException ex) {
            // Bad IteratorState serialization.
            throw new IllegalDataFormatException("Unable to deserialize `serializedState`.", ex);
        }

        if (state != null && state.isReachedEnd()) {
            // Nothing to iterate on.
            return CompletableFuture.completedFuture(TableIterator.empty());
        }

        val segmentKeyLength = getSegmentKeyLength(segment.getInfo());
        val fromId = state == null
                ? AttributeId.Variable.minValue(segmentKeyLength)
                : AttributeId.from(state.getLastKey().getCopy()).nextValue();
        val toId = AttributeId.Variable.maxValue(segmentKeyLength);
        val timer = new TimeoutTimer(args.getFetchTimeout());
        return segment.attributeIterator(fromId, toId, timer.getRemaining())
                .thenApply(ai -> ai.thenCompose(attributePairs -> {
                    // Create a new state.
                    val stateBuilder = IteratorStateImpl.builder();
                    CompletableFuture<List<T>> result;
                    if (attributePairs.isEmpty()) {
                        stateBuilder.reachedEnd(true);
                        result = CompletableFuture.completedFuture(Collections.emptyList());
                    } else {
                        stateBuilder.lastKey(attributePairs.get(attributePairs.size() - 1).getKey().toBuffer());
                        result = getItems.apply(segment, attributePairs, timer);
                    }

                    // TODO: truncated segment (if compaction)
                    return result.thenApply(items -> new IteratorItemImpl<>(stateBuilder.build().serialize(), items));
                }));
    }

    private void ensureSegmentType(String segmentName, SegmentType segmentType) {
        Preconditions.checkArgument(segmentType.isFixedKeyTableSegment(),
                "FixedKeyTableSegmentLayout can only be used for Fixed-Key Table Segments; Segment '%s' is '%s;.", segmentName, segmentType);
    }

    private void ensureValidKeyLength(String segmentName, int keyLength) {
        Preconditions.checkArgument(keyLength > 0 && keyLength <= AttributeId.MAX_KEY_LENGTH,
                "Segment KeyLength for segment `%s' must be a positive integer smaller than or equal to %s; actual %s.",
                segmentName, AttributeId.MAX_KEY_LENGTH, keyLength);
    }

    private int getSegmentKeyLength(SegmentProperties segmentInfo) {
        return (int) (long) segmentInfo.getAttributes().getOrDefault(Attributes.ATTRIBUTE_ID_LENGTH, -1L);
    }

    private CompletableFuture<List<TableKey>> getIteratorKeys(DirectSegmentAccess segment, List<Map.Entry<AttributeId, Long>> keys, TimeoutTimer timer) {
        return CompletableFuture.completedFuture(keys.stream().map(e -> TableKey.versioned(e.getKey().toBuffer(), e.getValue())).collect(Collectors.toList()));
    }

    private CompletableFuture<List<TableEntry>> getIteratorEntries(DirectSegmentAccess segment, List<Map.Entry<AttributeId, Long>> keys, TimeoutTimer timer) {
        return get(segment, keys.stream().map(e -> e.getKey().toBuffer()).collect(Collectors.toList()), timer);
    }

    //endregion

    //region Helper Classes

    @FunctionalInterface
    private interface GetIteratorItem<T> {
        CompletableFuture<List<T>> apply(DirectSegmentAccess segment, List<Map.Entry<AttributeId, Long>> keys, TimeoutTimer timer);
    }

    /**
     * Represents the state of a resumable iterator.
     */
    @RequiredArgsConstructor
    @Builder
    @Getter
    private static class IteratorStateImpl implements IteratorState {
        private static final Serializer SERIALIZER = new Serializer();

        @NonNull
        private final BufferView lastKey;
        private final boolean reachedEnd;

        @Override
        public String toString() {
            return String.format("LatKey = %s", this.lastKey);
        }

        //region Serialization

        /**
         * Creates a new instance of the IteratorState class from the given array.
         *
         * @param data The serialization of an IteratorState. This must have been generated using {@link #serialize()}.
         * @return As new instance of the IteratorState class.
         * @throws IOException If unable to deserialize.
         */
        static IteratorStateImpl deserialize(BufferView data) throws IOException {
            return SERIALIZER.deserialize(data);
        }

        /**
         * Serializes this IteratorState instance into an {@link ArrayView}.
         *
         * @return The {@link ArrayView} that was used for serialization.
         */
        @SneakyThrows(IOException.class)
        public ArrayView serialize() {
            return SERIALIZER.serialize(this);
        }

        private static class IteratorStateImplBuilder implements ObjectBuilder<IteratorStateImpl> {

        }

        private static class Serializer extends VersionedSerializer.WithBuilder<IteratorStateImpl, IteratorStateImpl.IteratorStateImplBuilder> {
            @Override
            protected IteratorStateImplBuilder newBuilder() {
                return new IteratorStateImplBuilder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput revisionDataInput, IteratorStateImplBuilder builder) throws IOException {
                builder.lastKey = new ByteArraySegment(revisionDataInput.readArray());
                builder.reachedEnd = revisionDataInput.readBoolean();
            }

            private void write00(IteratorStateImpl state, RevisionDataOutput revisionDataOutput) throws IOException {
                //revisionDataOutput.length(revisionDataOutput.getCompactIntLength(state.lastKey.getLength()) + state.lastKey.getLength() + 1);
                revisionDataOutput.writeBuffer(state.lastKey);
                revisionDataOutput.writeBoolean(state.reachedEnd);
            }
        }

        //endregion
    }

    //endregion

}
