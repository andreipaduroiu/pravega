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

import io.pravega.common.TimeoutTimer;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableSegmentConfig;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.WriterSegmentProcessor;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstraction for a general Table Segment Layout.
 */
@Slf4j
abstract class TableSegmentLayout implements AutoCloseable {
    /**
     * Default value used for when no offset is provided for a remove or put call.
     */
    protected static final long NO_OFFSET = -1;
    protected static final int MAX_BATCH_SIZE = 32 * EntrySerializer.MAX_SERIALIZATION_LENGTH;

    protected final SegmentContainer segmentContainer;
    protected final ScheduledExecutorService executor;
    protected final EntrySerializer serializer;
    protected final TableExtensionConfig config;
    protected final String traceObjectId;

    protected TableSegmentLayout(@NonNull SegmentContainer segmentContainer, @NonNull TableExtensionConfig config, @NonNull ScheduledExecutorService executor) {
        this.segmentContainer = segmentContainer;
        this.config = config;
        this.executor = executor;
        this.serializer = new EntrySerializer();
        this.traceObjectId = String.format("TableExtensionImpl[%s]", segmentContainer.getId());
    }

    @Override
    public abstract void close();

    abstract Collection<WriterSegmentProcessor> createWriterSegmentProcessors(UpdateableSegmentMetadata metadata);

    abstract void setNewSegmentAttributes(@NonNull SegmentType segmentType, @NonNull TableSegmentConfig config, @NonNull Map<AttributeId, Long> attributes);

    abstract CompletableFuture<Void> deleteSegment(@NonNull String segmentName, boolean mustBeEmpty, Duration timeout);

    abstract CompletableFuture<List<Long>> put(@NonNull DirectSegmentAccess segment, @NonNull List<TableEntry> entries, long tableSegmentOffset, TimeoutTimer timer);

    abstract CompletableFuture<Void> remove(@NonNull DirectSegmentAccess segment, @NonNull Collection<TableKey> keys, long tableSegmentOffset, TimeoutTimer timer);

    abstract CompletableFuture<List<TableEntry>> get(@NonNull DirectSegmentAccess segment, @NonNull List<BufferView> keys, TimeoutTimer timer);

    abstract CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(@NonNull DirectSegmentAccess segment, IteratorArgs args);

    abstract CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(@NonNull DirectSegmentAccess segment, IteratorArgs args);

    abstract AsyncIterator<IteratorItem<TableEntry>> entryDeltaIterator(@NonNull DirectSegmentAccess segment, long fromPosition, Duration fetchTimeout);

    protected TableEntry maybeDeleted(TableEntry e) {
        return e == null || e.getValue() == null ? null : e;
    }

    protected void logRequest(String requestName, Object... args) {
        log.debug("{}: {} {}", this.traceObjectId, requestName, args);
    }

    @Data
    protected static class IteratorItemImpl<T> implements IteratorItem<T> {
        private final BufferView state;
        private final Collection<T> entries;
    }

    public static class UpdateBatchTooLargeException extends IllegalArgumentException {
        UpdateBatchTooLargeException(int length, int maxLength) {
            super(String.format("Update Batch length %s exceeds the maximum limit %s.", length, maxLength));
        }
    }

}
