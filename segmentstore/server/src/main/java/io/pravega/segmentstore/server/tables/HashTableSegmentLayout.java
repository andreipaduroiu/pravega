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
import com.google.common.util.concurrent.Runnables;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.IllegalDataFormatException;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableSegmentConfig;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.WriterSegmentProcessor;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Hash-based Table Segment Layout.
 * (This is the classical Table Segment).
 */
class HashTableSegmentLayout extends TableSegmentLayout {
    private final KeyHasher hasher;
    private final ContainerSortedKeyIndex sortedKeyIndex;
    private final ContainerKeyIndex keyIndex;

    HashTableSegmentLayout(SegmentContainer segmentContainer, @NonNull CacheManager cacheManager, KeyHasher hasher,
                           Config config, ScheduledExecutorService executorService) {
        super(segmentContainer, config, executorService);
        this.hasher = hasher;
        this.sortedKeyIndex = createSortedIndex();
        this.keyIndex = new ContainerKeyIndex(segmentContainer.getId(), cacheManager, this.sortedKeyIndex, this.hasher, this.executor);
    }

    private ContainerSortedKeyIndex createSortedIndex() {
        val ds = new SortedKeyIndexDataSource(this::putInternal, this::removeInternal, this::getInternal);
        return new ContainerSortedKeyIndex(ds, this.executor);
    }

    @Override
    public void close() {
        this.keyIndex.close();
    }

    @Override
    Collection<WriterSegmentProcessor> createWriterSegmentProcessors(UpdateableSegmentMetadata metadata) {
        ensureSegmentType(metadata.getName(), metadata.getType());
        return Collections.singletonList(new WriterTableProcessor(new TableWriterConnectorImpl(metadata), this.executor));
    }

    @Override
    void setNewSegmentAttributes(@NonNull SegmentType segmentType, @NonNull TableSegmentConfig config, @NonNull Map<AttributeId, Long> attributes) {
        ensureSegmentType("", segmentType);
        if (segmentType.isSortedTableSegment()) {
            attributes.put(TableAttributes.SORTED, Attributes.BOOLEAN_TRUE);
        }
    }

    @Override
    CompletableFuture<Void> deleteSegment(@NonNull String segmentName, boolean mustBeEmpty, Duration timeout) {
        if (mustBeEmpty) {
            TimeoutTimer timer = new TimeoutTimer(timeout);
            return this.segmentContainer
                    .forSegment(segmentName, timer.getRemaining())
                    .thenComposeAsync(segment -> this.keyIndex.executeIfEmpty(segment,
                            () -> this.segmentContainer.deleteStreamSegment(segmentName, timer.getRemaining()), timer),
                            this.executor);
        }

        return this.segmentContainer.deleteStreamSegment(segmentName, timeout);
    }

    @Override
    CompletableFuture<List<Long>> put(@NonNull DirectSegmentAccess segment, @NonNull List<TableEntry> entries, long tableSegmentOffset, TimeoutTimer timer) {
        return put(segment, entries, true, tableSegmentOffset, timer);
    }

    private CompletableFuture<List<Long>> put(@NonNull DirectSegmentAccess segment, @NonNull List<TableEntry> entries, boolean external, long tableSegmentOffset, TimeoutTimer timer) {
        val segmentInfo = segment.getInfo();
        ensureSegmentType(segmentInfo.getName(), segmentInfo.getType());
        val toUpdate = translateItems(entries, segmentInfo, external, KeyTranslator::inbound);

        // Generate an Update Batch for all the entries (since we need to know their Key Hashes and relative
        // offsets in the batch itself).
        val updateBatch = batch(toUpdate, TableEntry::getKey, this.serializer::getUpdateLength, TableKeyBatch.update());
        logRequest("put", segmentInfo.getName(), updateBatch.isConditional(), tableSegmentOffset, updateBatch.isRemoval(),
                toUpdate.size(), updateBatch.getLength());
        return this.keyIndex.update(segment, updateBatch,
                () -> commit(toUpdate, this.serializer::serializeUpdate, segment, tableSegmentOffset, timer.getRemaining()), timer);
    }

    private CompletableFuture<List<Long>> putInternal(@NonNull String segmentName, List<TableEntry> entries, Duration timeout) {
        val timer = new TimeoutTimer(timeout);
        return this.segmentContainer.forSegment(segmentName, timer.getRemaining())
                .thenComposeAsync(s -> put(s, entries, false, NO_OFFSET, timer), this.executor);
    }

    @Override
    CompletableFuture<Void> remove(@NonNull DirectSegmentAccess segment, @NonNull Collection<TableKey> keys, long tableSegmentOffset, TimeoutTimer timer) {
        return remove(segment, keys, true, tableSegmentOffset, timer);
    }

    private CompletableFuture<Void> remove(@NonNull DirectSegmentAccess segment, @NonNull Collection<TableKey> keys, boolean external, long tableSegmentOffset, TimeoutTimer timer) {
        val segmentInfo = segment.getInfo();
        ensureSegmentType(segmentInfo.getName(), segmentInfo.getType());
        val toRemove = translateItems(keys, segmentInfo, external, KeyTranslator::inbound);
        val removeBatch = batch(toRemove, key -> key, this.serializer::getRemovalLength, TableKeyBatch.removal());
        logRequest("remove", segmentInfo.getName(), removeBatch.isConditional(), removeBatch.isRemoval(),
                toRemove.size(), removeBatch.getLength());
        return this.keyIndex.update(segment, removeBatch,
                () -> commit(toRemove, this.serializer::serializeRemoval, segment, tableSegmentOffset, timer.getRemaining()), timer)
                .thenRun(Runnables.doNothing());
    }

    private CompletableFuture<Void> removeInternal(@NonNull String segmentName, Collection<TableKey> keys, Duration timeout) {
        val timer = new TimeoutTimer(timeout);
        return this.segmentContainer.forSegment(segmentName, timer.getRemaining())
                .thenComposeAsync(s -> remove(s, keys, false, NO_OFFSET, timer), this.executor);
    }

    @Override
    CompletableFuture<List<TableEntry>> get(@NonNull DirectSegmentAccess segment, @NonNull List<BufferView> keys, TimeoutTimer timer) {
        return get(segment, keys, true, timer);
    }

    private CompletableFuture<List<TableEntry>> get(@NonNull DirectSegmentAccess segment, @NonNull List<BufferView> keys, boolean external, TimeoutTimer timer) {
        val segmentInfo = segment.getInfo();
        ensureSegmentType(segmentInfo.getName(), segmentInfo.getType());
        val toGet = translateItems(keys, segmentInfo, external, KeyTranslator::inbound);
        val resultBuilder = new GetResultBuilder(toGet, this.hasher);
        return this.keyIndex.getBucketOffsets(segment, resultBuilder.getHashes(), timer)
                .thenComposeAsync(offsets -> get(segment, resultBuilder, offsets, timer), this.executor)
                .thenApply(results -> translateItems(results, segmentInfo, external, KeyTranslator::outbound));
    }

    private CompletableFuture<List<TableEntry>> get(DirectSegmentAccess segment, GetResultBuilder builder,
                                                    Map<UUID, Long> bucketOffsets, TimeoutTimer timer) {
        val bucketReader = TableBucketReader.entry(segment, this.keyIndex::getBackpointerOffset, this.executor);
        int resultSize = builder.getHashes().size();
        for (int i = 0; i < resultSize; i++) {
            UUID keyHash = builder.getHashes().get(i);
            long offset = bucketOffsets.get(keyHash);
            if (offset == TableKey.NOT_EXISTS) {
                // Bucket does not exist, hence neither does the key.
                builder.includeResult(CompletableFuture.completedFuture(null));
            } else {
                // Find the sought entry in the segment, based on its key.
                BufferView key = builder.getKeys().get(i);
                builder.includeResult(this.keyIndex.findBucketEntry(segment, bucketReader, key, offset, timer).thenApply(this::maybeDeleted));
            }
        }

        return builder.getResultFutures();
    }

    private CompletableFuture<List<TableEntry>> getInternal(@NonNull String segmentName, List<BufferView> keys, Duration timeout) {
        val timer = new TimeoutTimer(timeout);
        return this.segmentContainer.forSegment(segmentName, timer.getRemaining())
                .thenComposeAsync(s -> get(s, keys, false, timer), this.executor);
    }

    @Override
    CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(@NonNull DirectSegmentAccess segment, IteratorArgs args) {
        val segmentInfo = segment.getInfo();
        ensureSegmentType(segmentInfo.getName(), segmentInfo.getType());
        if (ContainerSortedKeyIndex.isSortedTableSegment(segmentInfo)) {
            logRequest("keyIterator", segmentInfo.getName(), "sorted");
            return newSortedIterator(segment, args,
                    keys -> CompletableFuture.completedFuture(keys.stream().map(TableKey::unversioned).collect(Collectors.toList())));
        } else {
            logRequest("keyIterator", segmentInfo.getName(), "hash");
            return newHashIterator(segment, args, TableBucketReader::key, KeyTranslator::outbound);
        }
    }

    @Override
    CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(@NonNull DirectSegmentAccess segment, IteratorArgs args) {
        val segmentInfo = segment.getInfo();
        ensureSegmentType(segmentInfo.getName(), segmentInfo.getType());
        if (ContainerSortedKeyIndex.isSortedTableSegment(segmentInfo)) {
            logRequest("entryIterator", segmentInfo.getName(), "sorted");
            return newSortedIterator(segment, args, keys -> get(segment, keys, new TimeoutTimer(args.getFetchTimeout())));
        } else {
            logRequest("entryIterator", segmentInfo.getName(), "hash");
            return newHashIterator(segment, args, TableBucketReader::entry, KeyTranslator::outbound);
        }
    }

    @Override
    AsyncIterator<IteratorItem<TableEntry>> entryDeltaIterator(@NonNull DirectSegmentAccess segment, long fromPosition, Duration fetchTimeout) {
        val segmentInfo = segment.getInfo();
        ensureSegmentType(segmentInfo.getName(), segmentInfo.getType());
        if (ContainerSortedKeyIndex.isSortedTableSegment(segmentInfo)) {
            throw new UnsupportedOperationException("Unable to use a delta iterator on a sorted TableSegment.");
        }
        Preconditions.checkArgument(fromPosition <= segmentInfo.getLength(), "fromPosition (%s) can not exceed the length (%s) of the TableSegment.",
                fromPosition, segmentInfo.getLength());

        logRequest("entryDeltaIterator", segment.getSegmentId(), fromPosition);

        long compactionOffset = segmentInfo.getAttributes().getOrDefault(TableAttributes.COMPACTION_OFFSET, 0L);
        // All of the most recent keys will exist beyond the compactionOffset.
        long startOffset = Math.max(fromPosition, compactionOffset);
        // We should clear if the starting position may have been truncated out due to compaction.
        boolean shouldClear = fromPosition < compactionOffset;
        // Maximum length of the TableSegment we want to read until.
        int maxBytesToRead = (int) (segmentInfo.getLength() - startOffset);
        TableEntryDeltaIterator.ConvertResult<IteratorItem<TableEntry>> converter = item ->
                CompletableFuture.completedFuture(new IteratorItemImpl<TableEntry>(
                        item.getKey().serialize(),
                        Collections.singletonList(item.getValue())));
        return TableEntryDeltaIterator.<IteratorItem<TableEntry>>builder()
                .segment(segment)
                .entrySerializer(serializer)
                .executor(executor)
                .maxBytesToRead(maxBytesToRead)
                .startOffset(startOffset)
                .currentBatchOffset(fromPosition)
                .fetchTimeout(fetchTimeout)
                .resultConverter(converter)
                .shouldClear(shouldClear)
                .build();
    }

    private <T> CompletableFuture<AsyncIterator<IteratorItem<T>>> newSortedIterator(@NonNull DirectSegmentAccess segment, @NonNull IteratorArgs args,
                                                                                    @NonNull Function<List<BufferView>, CompletableFuture<List<T>>> toResult) {
        return this.keyIndex.getSortedKeyIndex(segment)
                .thenApply(index -> {
                    val prefix = translateItem(args.getPrefixFilter(), SortedKeyIndexDataSource.EXTERNAL_TRANSLATOR, KeyTranslator::inbound);
                    val range = index.getIteratorRange(args.getSerializedState(), prefix);
                    return index.iterator(range, args.getFetchTimeout())
                            .thenCompose(keys -> toSortedIteratorItem(keys, toResult, segment.getInfo()));
                });
    }

    private <T> CompletableFuture<IteratorItem<T>> toSortedIteratorItem(List<BufferView> keys, Function<List<BufferView>,
            CompletableFuture<List<T>>> toResult, SegmentProperties segmentInfo) {
        if (keys == null || keys.isEmpty()) {
            // End of iteration.
            return CompletableFuture.completedFuture(null);
        }

        // Remember the last key before the translation. We'll send this with the response so we know where to resume next.
        val lastKey = keys.get(keys.size() - 1);

        // Convert the Keys to their external form.
        keys = translateItems(keys, segmentInfo, true, KeyTranslator::outbound);

        // Get the result and include it in the response.
        return toResult.apply(keys)
                .thenApply(result -> {
                    // Some elements may have been deleted in the meantime, so exclude them.
                    result = result.stream().filter(Objects::nonNull).collect(Collectors.toList());
                    return new IteratorItemImpl<>(lastKey, result);
                });
    }

    private <T> CompletableFuture<AsyncIterator<IteratorItem<T>>> newHashIterator(@NonNull DirectSegmentAccess segment, @NonNull IteratorArgs args,
                                                                                  @NonNull GetBucketReader<T> createBucketReader,
                                                                                  @NonNull BiFunction<KeyTranslator, T, T> translateItem) {
        Preconditions.checkArgument(args.getPrefixFilter() == null, "Cannot perform a KeyHash iteration with a prefix.");
        UUID fromHash;
        try {
            fromHash = KeyHasher.getNextHash(args.getSerializedState() == null ? null : IteratorStateImpl.deserialize(args.getSerializedState()).getKeyHash());
        } catch (IOException ex) {
            // Bad IteratorState serialization.
            throw new IllegalDataFormatException("Unable to deserialize `serializedState`.", ex);
        }

        if (fromHash == null) {
            // Nothing to iterate on.
            return CompletableFuture.completedFuture(TableIterator.empty());
        }

        // Create a converter that will use a TableBucketReader to fetch all requested items in the iterated Buckets.
        val bucketReader = createBucketReader.apply(segment, this.keyIndex::getBackpointerOffset, this.executor);
        val segmentInfo = segment.getInfo();
        TableIterator.ConvertResult<IteratorItem<T>> converter = bucket ->
                bucketReader.findAllExisting(bucket.getSegmentOffset(), new TimeoutTimer(args.getFetchTimeout()))
                        .thenApply(result -> {
                            result = translateItems(result, segmentInfo, true, translateItem);
                            return new IteratorItemImpl<>(new IteratorStateImpl(bucket.getHash()).serialize(), result);
                        });

        // Fetch the Tail (Unindexed) Hashes, then create the TableIterator.
        return this.keyIndex.getUnindexedKeyHashes(segment)
                .thenComposeAsync(cacheHashes -> TableIterator.<IteratorItem<T>>builder()
                        .segment(segment)
                        .cacheHashes(cacheHashes)
                        .firstHash(fromHash)
                        .executor(executor)
                        .resultConverter(converter)
                        .fetchTimeout(args.getFetchTimeout())
                        .build(), this.executor);
    }

    private <T> TableKeyBatch batch(Collection<T> toBatch, Function<T, TableKey> getKey, Function<T, Integer> getLength, TableKeyBatch batch) {
        for (T item : toBatch) {
            val length = getLength.apply(item);
            val key = getKey.apply(item);
            batch.add(key, this.hasher.hash(key.getKey()), length);
        }

        Preconditions.checkArgument(batch.getLength() <= MAX_BATCH_SIZE,
                "Update Batch length (%s) exceeds the maximum limit.", MAX_BATCH_SIZE);
        return batch;
    }

    private <T> CompletableFuture<Long> commit(Collection<T> toCommit, Function<Collection<T>, BufferView> serializer,
                                               DirectSegmentAccess segment, long tableSegmentOffset, Duration timeout) {
        BufferView s = serializer.apply(toCommit);
        if (tableSegmentOffset == NO_OFFSET) {
            return segment.append(s, null, timeout);
        } else {
            return segment.append(s, null, tableSegmentOffset, timeout);
        }
    }

    @SuppressWarnings("unchecked")
    private <T, V extends Collection<T>> V translateItems(V items, SegmentProperties segmentInfo, boolean isExternal,
                                                          BiFunction<KeyTranslator, T, T> translateFunction) {
        if (!ContainerSortedKeyIndex.isSortedTableSegment(segmentInfo)) {
            // Nothing to translate for non-sorted segments.
            return items;
        }

        val t = isExternal ? SortedKeyIndexDataSource.EXTERNAL_TRANSLATOR : SortedKeyIndexDataSource.INTERNAL_TRANSLATOR;
        return (V) items.stream().map(i -> translateItem(i, t, translateFunction)).collect(Collectors.toList());
    }

    private <T> T translateItem(T item, KeyTranslator translator, BiFunction<KeyTranslator, T, T> translateItem) {
        return item == null ? null : translateItem.apply(translator, item);
    }

    private void ensureSegmentType(String segmentName, SegmentType segmentType) {
        Preconditions.checkArgument(segmentType.isTableSegment() && !segmentType.isFixedKeyTableSegment(),
                "HashTableSegment can only be used for variable-key Table Segments; Segment '%s' is '%s;.", segmentName, segmentType);
    }

    //region TableWriterConnector

    @RequiredArgsConstructor
    private class TableWriterConnectorImpl implements TableWriterConnector {
        @Getter
        private final SegmentMetadata metadata;

        @Override
        public EntrySerializer getSerializer() {
            return HashTableSegmentLayout.this.serializer;
        }

        @Override
        public KeyHasher getKeyHasher() {
            return HashTableSegmentLayout.this.hasher;
        }

        @Override
        public SegmentSortedKeyIndex getSortedKeyIndex() {
            return HashTableSegmentLayout.this.sortedKeyIndex.getSortedKeyIndex(this.metadata.getId(), this.metadata);
        }

        @Override
        public CompletableFuture<DirectSegmentAccess> getSegment(Duration timeout) {
            return HashTableSegmentLayout.this.segmentContainer.forSegment(this.metadata.getName(), timeout);
        }

        @Override
        public void notifyIndexOffsetChanged(long lastIndexedOffset) {
            HashTableSegmentLayout.this.keyIndex.notifyIndexOffsetChanged(this.metadata.getId(), lastIndexedOffset);
        }

        @Override
        public int getMaxCompactionSize() {
            return HashTableSegmentLayout.this.config.getMaxCompactionSize();
        }

        @Override
        public void close() {
            // Tell the KeyIndex that it's ok to clear any tail-end cache.
            HashTableSegmentLayout.this.keyIndex.notifyIndexOffsetChanged(this.metadata.getId(), -1L);
        }
    }

    //endregion

    //region GetResultBuilder

    /**
     * Helps build Result for the {@link #get} method.
     */
    private static class GetResultBuilder {
        /**
         * Sought keys.
         */
        @Getter
        private final List<BufferView> keys;
        /**
         * Sought keys's hashes, in the same order as the keys.
         */
        @Getter
        private final List<UUID> hashes;

        /**
         * A list of Futures with the results for each key, in the same order as the keys.
         */
        private final List<CompletableFuture<TableEntry>> resultFutures;

        GetResultBuilder(List<BufferView> keys, KeyHasher hasher) {
            this.keys = keys;
            this.hashes = keys.stream().map(hasher::hash).collect(Collectors.toList());
            this.resultFutures = new ArrayList<>();
        }

        void includeResult(CompletableFuture<TableEntry> entryFuture) {
            this.resultFutures.add(entryFuture);
        }

        CompletableFuture<List<TableEntry>> getResultFutures() {
            return Futures.allOfWithResults(this.resultFutures);
        }
    }

    //endregion

    //region IteratorItemImpl

    @FunctionalInterface
    private interface GetBucketReader<T> {
        TableBucketReader<T> apply(DirectSegmentAccess segment, TableBucketReader.GetBackpointer getBackpointer, ScheduledExecutorService executor);
    }

    //endregion

}
