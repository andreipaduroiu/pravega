/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.hash;

import com.google.common.base.Preconditions;
import io.pravega.common.util.ArrayView;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.val;

@NotThreadSafe
public class Entropy {
    private static final int MAX_DICTIONARY_SIZE = Byte.MAX_VALUE - Byte.MIN_VALUE + 1; // 255
    private final int dictionarySize;
    private final int[] biGrams;
    private int biGramCount;

    public Entropy() {
        this(MAX_DICTIONARY_SIZE);
    }

    public Entropy(int dictionarySize) {
        Preconditions.checkArgument(dictionarySize > 0 && dictionarySize <= MAX_DICTIONARY_SIZE,
                "dictionarySize must be a positive integer less than or equal to %s.", MAX_DICTIONARY_SIZE);
        this.dictionarySize = dictionarySize;
        this.biGrams = new int[this.dictionarySize * this.dictionarySize];
        this.biGramCount = 0;
    }

    public void include(ArrayView array, int sampleCount, int sampleSize) {
        int sampleRegionSize = array.getLength() / sampleCount;
        sampleSize = Math.min(sampleSize, sampleRegionSize);
        for (int i = 0; i < sampleCount; i++) {
            val sample = (ArrayView) array.slice(i * sampleRegionSize, sampleSize);
            include(sample);
        }
    }

    public void include(ArrayView array) {
        for (int i = 1; i < array.getLength(); i++) {
            byte b1 = array.get(i - 1);
            byte b2 = array.get(i);
            int pairIdx = (b1 + 128) * this.dictionarySize + (b2 + 128);
            this.biGrams[pairIdx]++;
            this.biGramCount++;
        }
    }

    public double getEntropy() {
        double entropy = 0;
        for (int c : this.biGrams) {
            if (c != 0) {
                double p = (double) c / this.biGramCount;
                entropy += p * Math.log(p);
            }
        }

        return -entropy;
    }
}
