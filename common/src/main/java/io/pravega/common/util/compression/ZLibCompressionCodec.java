/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.compression;

import io.pravega.common.util.ArrayView;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import lombok.SneakyThrows;

public class ZLibCompressionCodec implements CompressionCodec {
    @Override
    public MultiInputCompressor compressTo(ArrayView output) {
        return new ZLibCompressor(output);
    }

    @Override
    @SneakyThrows(DataFormatException.class)
    public int decompress(ArrayView input, ArrayView output) {
        Inflater inflater = new Inflater();
        inflater.setInput(input.array(), input.arrayOffset(), input.getLength());
        int index = 0;
        int numBytes;
        do {
            numBytes = inflater.inflate(output.array(), output.arrayOffset() + index, output.getLength() - index);
            index += numBytes;
        } while (numBytes > 0);
        inflater.end();
        return index;
    }

    private static class ZLibCompressor extends MultiInputCompressor {
        private final Deflater deflater = new Deflater();

        ZLibCompressor(ArrayView output) {
            super(output);
        }

        @Override
        public void include(ArrayView input) {
            this.deflater.setInput(input.array(), input.arrayOffset(), input.getLength());
            int numBytes;
            do {
                ArrayView output = getRemainingOutput();
                numBytes = this.deflater.deflate(output.array(),
                        output.arrayOffset(),
                        output.getLength(),
                        Deflater.SYNC_FLUSH);
                incrementCompressedLength(numBytes);
            } while (numBytes > 0);
        }
    }
}