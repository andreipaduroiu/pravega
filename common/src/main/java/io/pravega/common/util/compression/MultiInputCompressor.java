/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.compression;

import com.google.common.base.Preconditions;
import io.pravega.common.util.ArrayView;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;

@NotThreadSafe
public abstract class MultiInputCompressor {
    private final ArrayView output;
    @Getter
    private int compressedLength;

    MultiInputCompressor(ArrayView output) {
        this.output = output;
        this.compressedLength = 0;
    }

    public abstract void include(ArrayView input);

    public ArrayView getCompressedOutput() {
        return (ArrayView) this.output.slice(0, this.compressedLength);
    }

    ArrayView getRemainingOutput() {
        return (ArrayView) this.output.slice(this.compressedLength, this.output.getLength() - this.compressedLength);
    }

    void incrementCompressedLength(int count) {
        Preconditions.checkArgument(this.compressedLength + count <= this.output.getLength());
        this.compressedLength += count;
    }
}