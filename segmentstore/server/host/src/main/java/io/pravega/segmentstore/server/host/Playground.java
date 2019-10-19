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
import io.pravega.common.hash.Entropy;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.storage.impl.bookkeeper.Compressor;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;
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
        //val tests = generateSemiRandomData(1000, max, rnd);
        //val tests = generateRandomData(1000, max, rnd);
        val tests = generateFileData();
        int compressibleCount = 0;
        int compressibleCorrect = 0;
        int incompressibleCount = 0;
        int incompressibleCorrect = 0;

        long totalCheckNanos = 0;
        long totalCompressNanos = 0;
        val compressor = new Compressor();
        for (val a : tests) {
            val checkTimer = new Timer();
            val e = new Entropy();
            e.include(new ByteArraySegment(a), Compressor.SAMPLE_COUNT, Compressor.SAMPLE_SIZE);
            double entropy = e.getEntropy();
            boolean isCompressible = entropy < Compressor.ENTROPY_THRESHOLD;
            totalCheckNanos += checkTimer.getElapsedNanos();

            val compressTimer = new Timer();
            val bas = new ByteArrayOutputStream();
            val gzip = new GZIPOutputStream(bas);
            gzip.write(a);
            gzip.finish();
            gzip.close();
            totalCompressNanos += compressTimer.getElapsedNanos();
            double ratio = (double) bas.size() / a.length;
            System.out.println(String.format("L: %3.0f%%, E: %.3f IC: %s", ratio * 100, entropy, isCompressible));

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

    private static List<byte[]> generateFileData() {
        val paths = new ArrayList<String>();
        collectFilePaths("/home/andrei/src/pravega", paths);
        return paths.stream()
                    .map(p -> {
                        try {
                            return Files.readAllBytes(Paths.get(p));
                        } catch (Exception ex) {
                            throw new CompletionException(ex);
                        }
                    })
                    .collect(Collectors.toList());
    }

    private static void collectFilePaths(String rootDir, ArrayList<String> paths) {
        File[] faFiles = new File(rootDir).listFiles();
        for (File file : faFiles) {
            if (file.isDirectory()) {
                collectFilePaths(file.getAbsolutePath(), paths);
            } else if (file.length() > 4096) {
                paths.add(file.getPath());
            }
        }
    }

}
