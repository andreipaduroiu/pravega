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

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Attribute Id for Segments.
 */
public abstract class AttributeId implements Comparable<AttributeId> {

    /**
     * Creates a new {@link AttributeId.UUID} using the given bits.
     * @param mostSignificantBits The UUID's MSB.
     * @param leastSignificantBits The UUID's LSB.
     * @return A new instance of {@link AttributeId.UUID}.
     */
    public static AttributeId.UUID uuid(long mostSignificantBits, long leastSignificantBits) {
        return new UUID(mostSignificantBits, leastSignificantBits);
    }

    /**
     * Creates a new {@link AttributeId.UUID} using the given {@link UUID}.
     * @param uuid The {@link UUID} to use.
     * @return A new instance of {@link AttributeId.UUID}.
     */
    public static AttributeId fromUUID(java.util.UUID uuid) {
        return new UUID(uuid);
    }

    /**
     * Creates a new {@link AttributeId} with random content.
     * @return A new instance of {@link AttributeId}.
     */
    public static AttributeId randomUUID() { // TODO rename to random()
        return new UUID(java.util.UUID.randomUUID());
    }

    /**
     * Gets a value indicating the size, in bytes, of this {@link AttributeId}.
     * @return The size.
     */
    public abstract int byteCount();

    /**
     * Gets a value representing the Most Significant Bits for this {@link AttributeId}.
     * @return The MSB.
     */
    public abstract long getMostSignificantBits();

    /**
     * Gets a value representing the Least Significant Bits for this {@link AttributeId}.
     * @return The LSB.
     */
    public abstract long getLeastSignificantBits();

    /**
     * Gets a new {@link UUID} that contains the same information as this {@link AttributeId}.
     * @return A new {@link UUID}.
     */
    public abstract java.util.UUID toUUID();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract int hashCode();

    @Override
    public abstract int compareTo(AttributeId val);

    /**
     * A 16-byte {@link AttributeId} that maps to a {@link UUID}.
     */
    @Getter
    @RequiredArgsConstructor
    public static final class UUID extends AttributeId {
        private final long mostSignificantBits;
        private final long leastSignificantBits;

        public UUID(java.util.UUID uuid) {
            this(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        }

        public java.util.UUID toUUID() {
            return new java.util.UUID(this.mostSignificantBits, this.leastSignificantBits);
        }

        @Override
        public int byteCount() {
            return 16;
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
}
