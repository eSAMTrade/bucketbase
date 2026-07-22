package com.esamtrade.bucketbase;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RangeSeekableInputStreamTest {
    private record RangeCall(long offset, int length) {
    }

    @Test
    void bufferedReadsReuseOneBoundedRange() throws IOException {
        byte[] content = "0123456789abcdefghijklmnopqrstuvwxyz".getBytes();
        List<RangeCall> calls = new ArrayList<>();
        RangeSeekableInputStream stream = createStream(content, 8, calls);

        stream.seek(3);
        assertArrayEquals("34".getBytes(), stream.readNBytes(2));
        stream.seek(6);
        assertArrayEquals("67".getBytes(), stream.readNBytes(2));

        assertEquals(List.of(new RangeCall(3, 8)), calls);
    }

    @Test
    void zeroBufferUsesExactRangesAndSupportsSeeking() throws IOException {
        byte[] content = "0123456789abcdefghijklmnopqrstuvwxyz".getBytes();
        List<RangeCall> calls = new ArrayList<>();
        RangeSeekableInputStream stream = createStream(content, 0, calls);

        stream.seek(content.length - 4L);
        assertArrayEquals("wxyz".getBytes(), stream.readNBytes(8));
        assertEquals(content.length, stream.position());
        assertEquals(-1, stream.read());
        stream.seek(content.length + 10L);
        assertEquals(-1, stream.read());

        assertEquals(List.of(new RangeCall(content.length - 4L, 4)), calls);
    }

    @Test
    void largeReadIsNotEnlargedByBuffering() throws IOException {
        byte[] content = "0123456789abcdefghijklmnopqrstuvwxyz".getBytes();
        List<RangeCall> calls = new ArrayList<>();
        RangeSeekableInputStream stream = createStream(content, 8, calls);

        assertArrayEquals(Arrays.copyOf(content, 9), stream.readNBytes(9));

        assertEquals(List.of(new RangeCall(0, 9)), calls);
    }

    @Test
    void shortRangeAndClosedStreamFailClearly() throws IOException {
        RangeSeekableInputStream shortStream = new RangeSeekableInputStream(10, 0, (offset, length) -> new byte[length - 1]);
        assertThrows(IOException.class, () -> shortStream.readNBytes(10));

        RangeSeekableInputStream closedStream = new RangeSeekableInputStream(10, 0, (offset, length) -> new byte[length]);
        closedStream.close();
        assertThrows(IOException.class, closedStream::read);
        assertThrows(IOException.class, () -> closedStream.seek(0));
        assertThrows(IOException.class, closedStream::position);
    }

    @Test
    void invalidSizesAndNegativeSeekAreRejected() throws IOException {
        assertThrows(IllegalArgumentException.class, () -> new RangeSeekableInputStream(-1, 0, (offset, length) -> new byte[length]));
        assertThrows(IllegalArgumentException.class, () -> new RangeSeekableInputStream(1, -1, (offset, length) -> new byte[length]));

        RangeSeekableInputStream stream = new RangeSeekableInputStream(1, 0, (offset, length) -> new byte[length]);
        assertThrows(IOException.class, () -> stream.seek(-1));
        assertThrows(IllegalArgumentException.class, () -> new ObjectStream(new ByteArrayInputStream(new byte[1]), "object.bin"));
    }

    private static RangeSeekableInputStream createStream(byte[] content, int readBufferSize, List<RangeCall> calls) {
        return new RangeSeekableInputStream(content.length, readBufferSize, (offset, length) -> {
            calls.add(new RangeCall(offset, length));
            return Arrays.copyOfRange(content, Math.toIntExact(offset), Math.toIntExact(offset) + length);
        });
    }
}
