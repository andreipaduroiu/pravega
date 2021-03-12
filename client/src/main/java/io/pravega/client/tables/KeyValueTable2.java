/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables;

import io.pravega.client.stream.Serializer;
import io.pravega.client.tables.impl.IteratorStateImpl;
import io.pravega.common.util.AsyncIterator;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

public interface KeyValueTable2<PrimaryKeyType, SecondaryKeyType, ValueType> {

    // Conditional or unconditional based on TableEntry.isVersioned.
    CompletableFuture<Version> put(@NonNull TableEntry<PrimaryKeyType, SecondaryKeyType, ValueType> entry);

    // Conditional or unconditional based on TableEntry.isVersioned for each entry.
    // All entries must have the given Primary Key (we need this because we have to select the segment prior to iterating through the Iterable).
    CompletableFuture<List<Version>> putAll(@NonNull PrimaryKeyType primaryKey, @NonNull Iterable<TableEntry<PrimaryKeyType, SecondaryKeyType, ValueType>> entries);

    // Conditional or unconditional based on TableKey.isVersioned.
    CompletableFuture<Void> remove(@NonNull TableKey<PrimaryKeyType, SecondaryKeyType> key);

    // Conditional or unconditional based on TableKey.isVersioned for each key.
    // All keys must have the given PrimaryKey.
    CompletableFuture<Void> removeAll(@NonNull PrimaryKeyType primaryKey, @NonNull Iterable<TableKey<PrimaryKeyType, SecondaryKeyType>> keys);

    // Gets a single entry.
    CompletableFuture<VersionedTableEntry<PrimaryKeyType, SecondaryKeyType, ValueType>> get(@NonNull TableKey<PrimaryKeyType, SecondaryKeyType> key);

    // Gets entries for multiple keys.
    CompletableFuture<List<VersionedTableEntry<PrimaryKeyType, SecondaryKeyType, ValueType>>> getAll(@NonNull Iterable<TableKey<PrimaryKeyType, SecondaryKeyType>> keys);

    // See below for IteratorState builders.
    AsyncIterator<IteratorItem<VersionedTableKey<PrimaryKeyType, SecondaryKeyType>>> keyIterator(int maxKeysAtOnce, @Nullable IteratorState state);

    // See below for IteratorState builders.
    AsyncIterator<IteratorItem<VersionedTableEntry<PrimaryKeyType, SecondaryKeyType, ValueType>>> entryIterator(int maxEntriesAtOnce, @Nullable IteratorState state);


    // These interfaces will be put in their own files (this is just to have everything in one place)

    // A Key is made of a mandatory Primary Key and optional Secondary Key.
    @Data
    @Builder
    class TableKey<PrimaryType, SecondaryType> {
        @NonNull
        private final PrimaryType primary;
        private final SecondaryType secondary;

        boolean isVersioned() {
            return false;
        }
    }

    // We don't need Versioned Key everywhere - this type will be used where needed.
    @Builder
    class VersionedTableKey<PrimaryType, SecondaryType> extends TableKey<PrimaryType, SecondaryType> {
        private final Version version;

        boolean isVersioned() {
            return true;
        }
    }

    // Key + Value.
    @Data
    @Builder
    class TableEntry<PrimaryType, SecondaryType, ValueT> {
        @NonNull
        protected final TableKey<PrimaryType, SecondaryType> key;
        private final ValueT value;

        boolean isVersioned() {
            return this.key.isVersioned();
        }
    }

    // We don't need Versioned everywhere - this type will be used where needed.
    @Builder
    class VersionedTableEntry<PrimaryType, SecondaryType, ValueT> extends TableEntry<PrimaryType, SecondaryType, ValueT> {
        // Forces the Key to be versioned.
        VersionedTableEntry(VersionedTableKey<PrimaryType, SecondaryType> key, ValueT value) {
            super(key, value);
        }

        @Override
        public VersionedTableKey<PrimaryType, SecondaryType> getKey() {
            return (VersionedTableKey<PrimaryType, SecondaryType>) super.key;
        }
    }

    interface IteratorState {
        ByteBuffer toBytes();

        boolean isEmpty();

        static io.pravega.client.tables.IteratorState fromBytes(ByteBuffer buffer) {
            return IteratorStateImpl.fromBytes(buffer);
        }

        // For Sorted Tables only: all keys with same PK.
        static <PK> IteratorState forPrimaryKey(@NonNull PK primaryKey, @Nullable ByteBuffer existingState) {
            return forKeyRange(primaryKey, null, null, existingState);
        }

        // For Sorted Tables only: all keys with same PK, between the two secondary keys.
        static <PK, SK> IteratorState forKeyRange(@NonNull PK primaryKey, @Nullable SK fromKey, @Nullable SK toKey, @Nullable ByteBuffer existingState) {
            return null;
        }
    }

    class KeySerializer<PK, SK, K extends TableKey<PK, SK>> implements Serializer<K> {
        @Override
        public ByteBuffer serialize(K key) {
            return null;
        }

        @Override
        public K deserialize(ByteBuffer serializedValue) {
            return null;
        }
    }
}
