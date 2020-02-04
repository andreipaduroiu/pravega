/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import io.pravega.common.util.compression.CompressionCodec;
import io.pravega.common.util.compression.MultiInputCompressor;
import io.pravega.common.util.compression.ZLibCompressionCodec;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
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
        final int preCompressSize = 32 * 1024;

        val codec = new ZLibCompressionCodec();

        //val tests = generateStructuredData(max, rnd);
        val tests = generateSemiRandomData(1000, max, rnd);
        //val tests = generateRandomData(1000, max, rnd);
        //val tests = generateFileData();
        int compressibleCount = 0;
        int compressibleCorrect = 0;
        int incompressibleCount = 0;
        int incompressibleCorrect = 0;

        long totalCheckNanos = 0;
        long totalCompressNanos = 0;
        int compressedCount = 0;
        for (val a : tests) {
            val input = new ByteArraySegment(a);

            val compressTimer = new Timer();
            val c1 = compress(codec, input, preCompressSize, compressibleRatio);
            boolean wasCompressed = c1 != null;
            if (wasCompressed) {
                totalCompressNanos += compressTimer.getElapsedNanos();
                compressedCount++;
            } else {
                totalCheckNanos += compressTimer.getElapsedNanos();
            }

            // Compress the whole thing anyway to verify compressibility.
            val actualCompressed = compress(codec, input, input.getLength(), 100.0);
            double ratio = (double) actualCompressed.getLength() / a.length;
            System.out.println(String.format("CR: %3.0f%%, Compressed: E = %s, A = %s",
                    ratio * 100, ratio <= compressibleRatio, wasCompressed));

            if (ratio <= compressibleRatio) {
                compressibleCount++;
                if (wasCompressed) {
                    compressibleCorrect++;
                }
            } else {
                incompressibleCount++;
                if (!wasCompressed) {
                    incompressibleCorrect++;
                }
            }
//
//            if (c1 != null) {
//                // Check that it was compressed correctly.
//                val c2 = new ByteArraySegment(new byte[max]);
//                int decompressedLength = codec.decompress(c1, c2);
//                if (decompressedLength != input.getLength()) {
//                    throw new Exception("decompressed size differs ");
//                }
//                for (int i = 0; i < input.getLength(); i++) {
//                    if (input.get(i) != c2.get(i)) {
//                        throw new Exception("differs at index " + i);
//                    }
//                }
//            }
        }

        System.out.println(String.format("\nAccuracy: Compressible = %s%% (%s), Incompressible = %s%% (%s)",
                (int) ((double) compressibleCorrect / compressibleCount * 100),
                compressibleCount,
                (int) ((double) incompressibleCorrect / incompressibleCount * 100),
                incompressibleCount));
        System.out.println(String.format("Time per compressed item (ms): %.1f", (double) totalCompressNanos / 1000_000 / compressedCount));
        System.out.println(String.format("Time per non-compressed item (ms): %.1f", (double) totalCheckNanos / 1000_000 / (tests.size() - compressedCount)));
    }


    private static ArrayView compress(CompressionCodec compressionCodec, ArrayView source, int checkThreshold, double maxRatio) {
        // TODO maybe smaller buffer for testing, then allocate big buffer? Maybe stitch two together?
        int sizeEstimate = (int) Math.ceil(source.getLength() * 1.001 + 14);
        ByteArraySegment compressed = new ByteArraySegment(new byte[sizeEstimate]);

        checkThreshold = Math.min(checkThreshold, source.getLength());

        MultiInputCompressor compressor = compressionCodec.compressTo(compressed);
        compressor.include((ArrayView) source.slice(0, checkThreshold));
        if ((double) compressor.getCompressedLength() / checkThreshold > maxRatio) {
            return null;
        } else if (checkThreshold < source.getLength()) {
            compressor.include((ArrayView) source.slice(checkThreshold, source.getLength() - checkThreshold));
        }

        return compressor.getCompressedOutput();
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
            byte[] data = new byte[max];
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
