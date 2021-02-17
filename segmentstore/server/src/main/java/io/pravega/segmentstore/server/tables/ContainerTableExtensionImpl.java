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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableSegmentConfig;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.WriterSegmentProcessor;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * A {@link ContainerTableExtension} that implements Table Segments on top of a {@link SegmentContainer}.
 */
@Slf4j
public class ContainerTableExtensionImpl implements ContainerTableExtension {
    //region Members

    /**
     * The default value to supply to a {@link WriterTableProcessor} to indicate how big compactions need to be.
     * We need to return a value that is large enough to encompass the largest possible Table Entry (otherwise
     * compaction will stall), but not too big, as that will introduce larger indexing pauses when compaction is running.
     */
    private static final int DEFAULT_MAX_COMPACTION_SIZE = 4 * EntrySerializer.MAX_SERIALIZATION_LENGTH; // Approx 4MB.
    /**
     * The default Segment Attributes to set for every new Table Segment. These values will override the corresponding
     * defaults from {@link TableAttributes#DEFAULT_VALUES}.
     */
    @VisibleForTesting
    static final Map<AttributeId, Long> DEFAULT_COMPACTION_ATTRIBUTES = ImmutableMap.of(TableAttributes.MIN_UTILIZATION, 75L,
            Attributes.ROLLOVER_SIZE, 4L * DEFAULT_MAX_COMPACTION_SIZE);

    private final SegmentContainer segmentContainer;
    private final ScheduledExecutorService executor;
    private final TableSegmentLayout.Config layoutConfig;
    private final HashTableSegmentLayout hashTableLayout;
    private final AtomicBoolean closed;
    private final String traceObjectId;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ContainerTableExtensionImpl class.
     *
     * @param segmentContainer The {@link SegmentContainer} to associate with.
     * @param cacheManager     The {@link CacheManager} to use to manage the cache.
     * @param executor         An Executor to use for async tasks.
     */
    public ContainerTableExtensionImpl(SegmentContainer segmentContainer, CacheManager cacheManager, ScheduledExecutorService executor) {
        this(segmentContainer, cacheManager, KeyHasher.sha256(), executor);
    }

    /**
     * Creates a new instance of the ContainerTableExtensionImpl class with custom {@link KeyHasher}.
     *
     * @param segmentContainer The {@link SegmentContainer} to associate with.
     * @param cacheManager     The {@link CacheManager} to use to manage the cache.
     * @param hasher           The {@link KeyHasher} to use.
     * @param executor         An Executor to use for async tasks.
     */
    @VisibleForTesting
    ContainerTableExtensionImpl(@NonNull SegmentContainer segmentContainer, @NonNull CacheManager cacheManager,
                                @NonNull KeyHasher hasher, @NonNull ScheduledExecutorService executor) {
        this.segmentContainer = segmentContainer;
        this.executor = executor;
        this.layoutConfig = new TableSegmentLayout.Config(DEFAULT_MAX_COMPACTION_SIZE);
        this.hashTableLayout = new HashTableSegmentLayout(this.segmentContainer, cacheManager, hasher, this.layoutConfig, this.executor);
        this.closed = new AtomicBoolean();
        this.traceObjectId = String.format("TableExtension[%d]", this.segmentContainer.getId());
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.hashTableLayout.close();
            log.info("{}: Closed.", this.traceObjectId);
        }
    }

    //endregion

    //region ContainerTableExtension Implementation

    @Override
    public Collection<WriterSegmentProcessor> createWriterSegmentProcessors(UpdateableSegmentMetadata metadata) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        if (!metadata.getType().isTableSegment()) {
            // Not a Table Segment; nothing to do.
            return Collections.emptyList();
        }

        return this.hashTableLayout.createWriterSegmentProcessors(metadata);
    }

    //endregion

    //region TableStore Implementation

    @Override
    public CompletableFuture<Void> createSegment(@NonNull String segmentName, SegmentType segmentType, TableSegmentConfig config, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        segmentType = SegmentType.builder(segmentType).tableSegment().build(); // Ensure at least a TableSegment type.
        val attributes = new HashMap<>(TableAttributes.DEFAULT_VALUES);
        attributes.putAll(DEFAULT_COMPACTION_ATTRIBUTES);
        this.hashTableLayout.setNewSegmentAttributes(segmentType, config, attributes);
        val attributeUpdates = attributes
                .entrySet().stream()
                .map(e -> new AttributeUpdate(e.getKey(), AttributeUpdateType.None, e.getValue()))
                .collect(Collectors.toList());
        logRequest("createSegment", segmentName, segmentType);
        return this.segmentContainer.createStreamSegment(segmentName, segmentType, attributeUpdates, timeout);
    }

    @Override
    public CompletableFuture<Void> deleteSegment(@NonNull String segmentName, boolean mustBeEmpty, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        logRequest("deleteSegment", segmentName, mustBeEmpty);
        return this.hashTableLayout.deleteSegment(segmentName, mustBeEmpty, timeout);
    }

    @Override
    public CompletableFuture<Void> merge(@NonNull String targetSegmentName, @NonNull String sourceSegmentName, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        throw new UnsupportedOperationException("merge");
    }

    @Override
    public CompletableFuture<Void> seal(String segmentName, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        throw new UnsupportedOperationException("seal");
    }

    @Override
    public CompletableFuture<List<Long>> put(@NonNull String segmentName, @NonNull List<TableEntry> entries, Duration timeout) {
        return put(segmentName, entries, TableSegmentLayout.NO_OFFSET, timeout);
    }

    @Override
    public CompletableFuture<List<Long>> put(@NonNull String segmentName, @NonNull List<TableEntry> entries, long tableSegmentOffset, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentContainer
                .forSegment(segmentName, timer.getRemaining())
                .thenComposeAsync(segment -> this.hashTableLayout.put(segment, entries, tableSegmentOffset, timer), this.executor);
    }

    @Override
    public CompletableFuture<Void> remove(@NonNull String segmentName, @NonNull Collection<TableKey> keys, Duration timeout) {
        return remove(segmentName, keys, TableSegmentLayout.NO_OFFSET, timeout);
    }

    @Override
    public CompletableFuture<Void> remove(@NonNull String segmentName, @NonNull Collection<TableKey> keys, long tableSegmentOffset, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentContainer
                .forSegment(segmentName, timer.getRemaining())
                .thenComposeAsync(segment -> this.hashTableLayout.remove(segment, keys, tableSegmentOffset, timer), this.executor);
    }

    @Override
    public CompletableFuture<List<TableEntry>> get(@NonNull String segmentName, @NonNull List<BufferView> keys, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentContainer
                .forSegment(segmentName, timer.getRemaining())
                .thenComposeAsync(segment -> this.hashTableLayout.get(segment, keys, timer), this.executor);
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(String segmentName, IteratorArgs args) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return this.segmentContainer.forSegment(segmentName, args.getFetchTimeout())
                .thenComposeAsync(segment -> this.hashTableLayout.keyIterator(segment, args), this.executor);
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(String segmentName, IteratorArgs args) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return this.segmentContainer.forSegment(segmentName, args.getFetchTimeout())
                .thenComposeAsync(segment -> this.hashTableLayout.entryIterator(segment, args), this.executor);
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryDeltaIterator(String segmentName, long fromPosition, Duration fetchTimeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return this.segmentContainer.forSegment(segmentName, fetchTimeout)
                .thenApplyAsync(segment -> this.hashTableLayout.entryDeltaIterator(segment, fromPosition, fetchTimeout), this.executor);
    }

    //endregion

    //region Helpers

    /**
     * Updates the {@link TableSegmentLayout.Config#getMaxCompactionSize()} with the given value.
     * By default this value is set to {@link #DEFAULT_MAX_COMPACTION_SIZE}.
     *
     * @param value The maximum length to compact at each step.
     */
    @VisibleForTesting
    protected void setMaxCompactionSize(int value) {
        this.layoutConfig.setMaxCompactionSize(value);
    }

    private void logRequest(String requestName, Object... args) {
        log.debug("{}: {} {}", this.traceObjectId, requestName, args);
    }

    //endregion

}
