/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import io.pravega.common.Timer;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.ByteArraySegment;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPOutputStream;
import lombok.Data;
import lombok.val;
import org.slf4j.LoggerFactory;

/**
 * Playground Test class.
 */
public class Playground {

    public static void main(String[] args) throws Exception {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.INFO);
        //context.reset();
        val rnd = new Random(0);
        final double compressibleRatio = 0.8;
        final int max = 1024 * 1024;

        //val tests = generateStructuredData(max, rnd);
        val tests = generateSemiRandomData(1000, max, rnd);
        //val tests = generateRandomData(1000, max, rnd);
        int compressibleCount = 0;
        int compressibleCorrect = 0;
        int incompressibleCount = 0;
        int incompressibleCorrect = 0;

        long includeNanos = 0;
        long totalCheckNanos = 0;
        long totalCompressNanos = 0;
        for (val a : tests) {
            val ic = new IsCompressible();
            val checkTimer = new Timer();
            ic.include(new ByteArraySegment(a), 8);
            includeNanos += checkTimer.getElapsedNanos();

            val eval = ic.evaluate();
            boolean isCompressible = eval.isCompressible();
            totalCheckNanos += checkTimer.getElapsedNanos();

            val compressTimer = new Timer();
            val bas = new ByteArrayOutputStream();
            val gzip = new GZIPOutputStream(bas);
            gzip.write(a);
            gzip.finish();
            totalCompressNanos += compressTimer.getElapsedNanos();
            double ratio = (double) bas.size() / a.length;
//            System.out.println(String.format("L: %3.0f%%, E: %.3f IC: %s",
//                    ratio * 100, eval.getEntropy(), eval.isCompressible()));

            if (ratio <= compressibleRatio) {
                compressibleCount++;
                if (isCompressible) {
                    compressibleCorrect++;
                }
            } else {
                incompressibleCount++;
                if (!isCompressible) {
                    incompressibleCorrect++;
                }
            }
        }

        System.out.println(String.format("\nAccuracy: Compressible = %s%% (%s), Incompressible = %s%% (%s)",
                (int) ((double) compressibleCorrect / compressibleCount * 100),
                compressibleCount,
                (int) ((double) incompressibleCorrect / incompressibleCount * 100),
                incompressibleCount));
        System.out.println(String.format("Include time per item (ms): %.1f", (double) includeNanos / 1000_000 / tests.size()));
        System.out.println(String.format("Eval time per item (ms): %.1f", (double) totalCheckNanos / 1000_000 / tests.size()));
        System.out.println(String.format("Compress time per item (ms): %.1f", (double) totalCompressNanos / 1000_000 / tests.size()));
    }

    private static List<byte[]> generateStructuredData(int max, Random rnd) {
        val constant = new byte[max];
        val every4 = new byte[max];
        val every32 = new byte[max];
        val every128 = new byte[max];
        val every256 = new byte[max];
        val random4 = new byte[max];
        val random32 = new byte[max];
        val random128 = new byte[max];
        val random256 = new byte[max];
        rnd.nextBytes(random256);
        for (int i = 0; i < max; i++) {
            every4[i] = (byte) (i % 4);
            every32[i] = (byte) (i % 32);
            every128[i] = (byte) (i % 128);
            every256[i] = (byte) (i % 256);
            int r = rnd.nextInt();
            random4[i] = (byte) (r % 4);
            random32[i] = (byte) (r % 32);
            random128[i] = (byte) (r % 128);
        }

        return Arrays.asList(
                constant,
                every4,
                every32,
                every128,
                every256,
                random4,
                random32,
                random128,
                random256
        );
    }

    private static List<byte[]> generateSemiRandomData(int count, int max, Random rnd) {
        val result = new ArrayList<byte[]>();
        int blockSize = 64;
        int currentSize = blockSize;
        for (int i = 0; i < count; i++) {
            byte[] data = new byte[max];
            byte[] block = new byte[currentSize];
            rnd.nextBytes(block);
            int j = 0;
            while (j < data.length) {
                int len = Math.min(data.length - j, block.length);
                System.arraycopy(block, 0, data, j, len);
                j += len;
            }
            result.add(data);
            currentSize += blockSize;
        }
        return result;
    }

    private static List<byte[]> generateRandomData(int count, int max, Random rnd) {
        val result = new ArrayList<byte[]>();
        for (int i = 0; i < count; i++) {
            byte[] data = new byte[rnd.nextInt(max) + 1];
            rnd.nextBytes(data);
            result.add(data);
        }
        return result;
    }

    private static class IsCompressible {
        private final int DICTIONARY_SIZE = 256;
        private final int DATA_LENGTH_THRESHOLD = 512;
        private final double ENTROPY_THRESHOLD = 10;
        private final int[] pairCounts = new int[DICTIONARY_SIZE * DICTIONARY_SIZE];
        private int totalPairCount = 0;

        private final int MAX_SAMPLE_SIZE = 16 * 1024;

        void include(ArrayView array, int sampleCount) {
            int sampleRegionSize = array.getLength() / sampleCount;
            for (int i = 0; i < sampleCount; i++) {
                val sample = (ArrayView) array.slice(i * sampleRegionSize, Math.min(MAX_SAMPLE_SIZE, sampleRegionSize));
                include(sample);
            }
        }

        void include(ArrayView array) {
            for (int i = 1; i < array.getLength(); i++) {
                totalPairCount++;
                byte a = array.get(i - 1);
                byte b = array.get(i);
                int pairIdx = ((a + 128) % DICTIONARY_SIZE) * DICTIONARY_SIZE + ((b + 128) % DICTIONARY_SIZE);
                pairCounts[pairIdx]++;
            }
        }


        EvaluationResult evaluate() {
            double entropy = 0;
            for (int c : this.pairCounts) {
                if (c != 0) {
                    double p = (double) c / totalPairCount;
                    entropy += p * Math.log(p);
                }
            }

            entropy *= -1;

            boolean isCompressible = isCompressible(Integer.MAX_VALUE, entropy);
            return new EvaluationResult(entropy, isCompressible);
        }


        private boolean isCompressible(int length, double entropy) {
            if (length < DATA_LENGTH_THRESHOLD) {
                return false;
            }

            if (entropy < ENTROPY_THRESHOLD) {
                return true;
            }

            return false;
        }


        @Data
        static class EvaluationResult {
            final double entropy;
            final boolean compressible;
        }
    }
}
