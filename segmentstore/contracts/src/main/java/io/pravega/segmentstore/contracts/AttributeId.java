/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.util.BufferViewComparator;
import io.pravega.common.util.ByteArraySegment;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Attribute Id for Segments.
 */
public abstract class AttributeId implements Comparable<AttributeId> {
    public static final int MAX_KEY_LENGTH = 256; // Maximum Attribute Id length is 256 bytes.
    private static final int UUID_KEY_LENGTH = 2 * Long.BYTES;

    /**
     * Creates a new {@link AttributeId.UUID} using the given bits.
     *
     * @param mostSignificantBits  The UUID's MSB.
     * @param leastSignificantBits The UUID's LSB.
     * @return A new instance of {@link AttributeId.UUID}.
     */
    public static AttributeId uuid(long mostSignificantBits, long leastSignificantBits) {
        return new UUID(mostSignificantBits, leastSignificantBits);
    }

    /**
     * Creates a new {@link AttributeId.UUID} using the given {@link UUID}.
     *
     * @param uuid The {@link UUID} to use.
     * @return A new instance of {@link AttributeId.UUID}.
     */
    public static AttributeId fromUUID(java.util.UUID uuid) {
        return new UUID(uuid);
    }

    /**
     * Creates a new {@link AttributeId.Variable} using the given {@link ByteArraySegment}.
     *
     * @param data The {@link ByteArraySegment} to wrap. NOTE: this will not be duplicated. Any changes to the underlying
     *             buffer will be reflected in this Attribute, which may have unintended consequences.
     * @return A new instance of {@link AttributeId.Variable}.
     */
    public static AttributeId from(ByteArraySegment data) {
        return new Variable(data);
    }

    /**
     * Creates a new {@link AttributeId.UUID} with random content.
     *
     * @return A new instance of {@link AttributeId.UUID} that is generated using {@link java.util.UUID#randomUUID()}.
     */
    @VisibleForTesting
    public static AttributeId randomUUID() {
        return new UUID(java.util.UUID.randomUUID());
    }

    public abstract boolean isUUID();

    /**
     * Gets a value indicating the size, in bytes, of this {@link AttributeId}.
     *
     * @return The size.
     */
    public abstract int byteCount();

    /**
     * Gets a 64-bit value representing the Bits beginning at the given index.
     *
     * @param position The 0-based position representing the bit group to get. Each position maps to an 8-byte (64-bit)
     *                 block of data (position 0 maps to index 0, position 1 maps to index 8, ..., position i maps to index 8*i).
     * @return The bit group.
     */
    public abstract long getBitGroup(int position);

    public java.util.UUID toUUID() {
        if (!isUUID()) {
            throw new UnsupportedOperationException(String.format("toUUID() invoked with byteCount() == %s. Required: %s.", byteCount(), UUID_KEY_LENGTH));
        }
        return new java.util.UUID(getBitGroup(0), getBitGroup(1));
    }

    /**
     * TODO: explain this is not appropriate for serialization and that it doesn't use unsigned. This should only be used
     * for non-UUID too.
     * @return
     */
    public abstract ByteArraySegment toBuffer();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract int hashCode();

    @Override
    public abstract int compareTo(AttributeId val);

    //region UUID

    /**
     * A 16-byte {@link AttributeId} that maps to a {@link UUID}.
     */
    @Getter
    @RequiredArgsConstructor
    static final class UUID extends AttributeId {
        private final long mostSignificantBits;
        private final long leastSignificantBits;

        public UUID(java.util.UUID uuid) {
            this(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        }

        @Override
        public boolean isUUID() {
            return true;
        }

        @Override
        public int byteCount() {
            return UUID_KEY_LENGTH;
        }

        @Override
        public long getBitGroup(int position) {
            switch (position) {
                case 0:
                    return this.mostSignificantBits;
                case 1:
                    return this.leastSignificantBits;
                default:
                    throw new IllegalArgumentException(this.getClass().getName() + " only supports bit groups 0 and 1. Requested: " + position);
            }
        }

        @Override
        public ByteArraySegment toBuffer() {
            ByteArraySegment result = new ByteArraySegment(new byte[UUID_KEY_LENGTH]);
            result.setLong(0, this.mostSignificantBits);
            result.setLong(Long.BYTES, this.leastSignificantBits);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof UUID) {
                UUID id = (UUID) obj;
                return this.mostSignificantBits == id.mostSignificantBits && this.leastSignificantBits == id.leastSignificantBits;
            }

            return false;
        }

        @Override
        public int hashCode() {
            long hash = this.mostSignificantBits ^ this.leastSignificantBits;
            return (int) (hash >> 32) ^ (int) hash;
        }

        @Override
        public int compareTo(AttributeId val) {
            UUID uuid = (UUID) val; // This will throw an exception if we try to compare the wrong types - it's OK.
            int r = Long.compare(this.mostSignificantBits, uuid.mostSignificantBits);
            if (r == 0) {
                r = Long.compare(this.leastSignificantBits, uuid.leastSignificantBits);
            }
            return r;
        }

        @Override
        public String toString() {
            return toUUID().toString();
        }
    }

    //endregion

    //region Variable


    /**
     * A 16-byte {@link AttributeId} that maps to a {@link UUID}.
     */
    @Getter
    @RequiredArgsConstructor
    static final class Variable extends AttributeId {
        private static final BufferViewComparator COMPARATOR = BufferViewComparator.create();
        @NonNull
        private final ByteArraySegment data; // TODO: ArrayView or BufferView? Or leave it as-is?

        @Override
        public boolean isUUID() {
            return this.data.getLength() == UUID_KEY_LENGTH;
        }

        @Override
        public int byteCount() {
            return this.data.getLength();
        }

        @Override
        public long getBitGroup(int position) {
            return this.data.getLong(position << 3); // This will do necessary checks for us.
        }

        @Override
        public ByteArraySegment toBuffer() {
            return this.data;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Variable) {
                return this.data.equals(((Variable) obj).data);
            }

            return false;
        }

        @Override
        public int hashCode() {
            return this.data.hashCode();
        }

        @Override
        public int compareTo(AttributeId val) {
            // This will throw an exception if we try to compare the wrong types - it's OK.
            return COMPARATOR.compare(this.data, ((Variable) val).data);
        }

        @Override
        public String toString() {
            return this.data.toString();
        }
    }

    //endregion
}
