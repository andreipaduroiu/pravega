/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.SimpleMovingAverage;
import io.pravega.common.Timer;
import io.pravega.common.hash.Entropy;
import io.pravega.common.io.EnhancedByteArrayOutputStream;
import io.pravega.common.util.ArrayView;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;
import lombok.SneakyThrows;
import lombok.val;

public class Compressor {
    @VisibleForTesting
    public static final int SAMPLE_SIZE = 16 * 1024;
    @VisibleForTesting
    public static final int SAMPLE_COUNT = 8;
    @VisibleForTesting
    public static final double ENTROPY_THRESHOLD = 8.8;
    private static final int SIZE_THRESHOLD = 512 * 1024;
    private static final int DEFAULT_COMPRESS_MILLIS = 25;

    private final SimpleMovingAverage recentCompressTimes = new SimpleMovingAverage(8);
    private final SimpleMovingAverage recentWriteTimes = new SimpleMovingAverage(8);

    public void recordWriteTime(int millis) {
        this.recentWriteTimes.add(millis);
    }

    @SneakyThrows(IOException.class)
    public ArrayView compressIfNecessary(ArrayView input) {
        if (shouldCompress(input)) {
            val timer = new Timer();
            EnhancedByteArrayOutputStream output = new EnhancedByteArrayOutputStream();
            try (GZIPOutputStream z = new GZIPOutputStream(output)) {
                z.write(input.array(), input.arrayOffset(), input.getLength());
                z.finish();
                this.recentCompressTimes.add((int) timer.getElapsedMillis());
                return output.getData();
            }
        } else {
            //System.out.println("NO COMPRESS: "+input.getLength());
            return input;
        }
    }

    public boolean shouldCompress(ArrayView input) {
        if (input.getLength() < SIZE_THRESHOLD) {
            // Too small.
            return false;
        }

        double writeMillis = this.recentWriteTimes.getAverage(0);
        double compressMillis = this.recentCompressTimes.getAverage(DEFAULT_COMPRESS_MILLIS);
        if (writeMillis < 2 * compressMillis) {
            // No point in compressing.
            //System.out.println(String.format("NO COMPRESS: W.Time: %s, C.Time: %s ", writeMillis, compressMillis));
            return false;
        }

        Entropy e = new Entropy();
        e.include(input, SAMPLE_COUNT, SAMPLE_SIZE);
        double entropy = e.getEntropy();
        if (entropy > ENTROPY_THRESHOLD) {
            // Too random
            return false;
        }

        return true;
    }
}
