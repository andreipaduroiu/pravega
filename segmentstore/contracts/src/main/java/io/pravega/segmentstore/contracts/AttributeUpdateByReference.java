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

import com.google.common.base.Preconditions;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Data;
import lombok.Getter;

/**
 * An {@link AttributeUpdate} that will set the value of an Attribute based on a reference to another Segment property.
 */
@NotThreadSafe
public class AttributeUpdateByReference extends AttributeUpdate {
    //region Members

    /**
     * True if the value has been calculated and set, false otherwise.
     * If true, then the value should not be re-evaluated again.
     */
    private boolean valueSet;
    /**
     * The {@link AttributeValueReference} to evaluate.
     */
    @Getter
    private final AttributeValueReference reference;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AttributeUpdateByReference class, except for ReplaceIfEquals.
     *
     * @param attributeId An {@link AttributeId} representing the ID of the attribute to update.
     * @param updateType  The UpdateType. All update types except ReplaceIfEquals work with this method.
     * @param reference   The AttributeValueReference to evaluate.
     */
    public AttributeUpdateByReference(AttributeId attributeId, AttributeUpdateType updateType, AttributeValueReference reference) {
        this(attributeId, updateType, reference, Attributes.NULL_ATTRIBUTE_VALUE);
    }

    /**
     * Creates a new instance of the AttributeUpdateByReference class.
     *
     * @param attributeId     An {@link AttributeId} representing the ID of the attribute to update.
     * @param updateType      The UpdateType.
     * @param reference       The AttributeValueReference to evaluate.
     * @param comparisonValue The value to compare against.
     */
    public AttributeUpdateByReference(AttributeId attributeId, AttributeUpdateType updateType, AttributeValueReference reference, long comparisonValue) {
        super(attributeId, updateType, Attributes.NULL_ATTRIBUTE_VALUE, comparisonValue);
        Preconditions.checkArgument(updateType != AttributeUpdateType.ReplaceIfGreater && updateType != AttributeUpdateType.Accumulate,
                "AttributeUpdateByReference not supported for ReplaceIfGreater or Accumulate.");
        this.reference = Preconditions.checkNotNull(reference, "reference");
    }

    //endregion

    //region AttributeUpdate overrides.

    @Override
    public void setValue(long value) {
        super.setValue(value);
        this.valueSet = true;
    }

    @Override
    public long getValue() {
        Preconditions.checkState(this.valueSet, "value not set");
        return super.getValue();
    }

    @Override
    public boolean equals(Object obj) {
        return this.valueSet && super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    //endregion

    //region AttributeValueReference and implementations

    /**
     * Defines a general reference for an attribute value.
     */
    public interface AttributeValueReference {
    }

    /**
     * Represents a {@link AttributeValueReference} that maps to the current length of the Segment.
     */
    @Data
    public static class SegmentLengthReference implements AttributeValueReference {
        /**
         * The offset to apply to the Segment's Length.
         * For example, if Segment Length is 100, and offset is -5, then this will evaluate to 100 - 5 = 95.
         */
        private final long offset;
    }

    //endregion
}