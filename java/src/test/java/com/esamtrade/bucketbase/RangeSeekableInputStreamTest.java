package com.esamtrade.bucketbase;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void largeReadIsFilledDirectlyWithoutEnlargement() throws IOException {
        byte[] content = "0123456789abcdefghijklmnopqrstuvwxyz".getBytes();
        List<RangeCall> calls = new ArrayList<>();
        RangeSeekableInputStream stream = createStream(content, 8, calls);

        assertArrayEquals(Arrays.copyOf(content, 9), stream.readNBytes(9));

        assertEquals(List.of(new RangeCall(0, 9)), calls);
    }

    @Test
    void markAndResetRestorePosition() throws IOException {
        byte[] content = "0123456789".getBytes();
        RangeSeekableInputStream stream = createStream(content, 0, new ArrayList<>());

        stream.seek(2);
        stream.mark(0);
        assertArrayEquals("234".getBytes(), stream.readNBytes(3));
        stream.reset();
        assertArrayEquals("234".getBytes(), stream.readNBytes(3));
    }

    @Test
    void availableReportsOnlyBufferedBytes() throws IOException {
        byte[] content = "0123456789abcdef".getBytes();
        RangeSeekableInputStream stream = createStream(content, 8, new ArrayList<>());

        // Nothing prefetched yet: no bytes are readable without a fetch.
        assertEquals(0, stream.available());
        stream.read();
        // One 8-byte range is now buffered; 7 remain ahead of the position.
        assertEquals(7, stream.available());
    }

    @Test
    void aFailingRangeReaderPropagates() {
        RangeSeekableInputStream stream = new RangeSeekableInputStream(10, 0,
                (offset, dest, destOffset, length) -> {
                    throw new IOException("boom");
                });
        assertThrows(IOException.class, () -> stream.readNBytes(10));
    }

    @Test
    void closeReleasesTheBackingResourceExactlyOnce() throws IOException {
        // FSBucket hands its open FileChannel over as the backing resource. Nothing else releases it, so closing the stream has to.
        AtomicInteger closeCount = new AtomicInteger();
        RangeSeekableInputStream stream = new RangeSeekableInputStream(4, 0,
                (offset, dest, destOffset, length) -> Arrays.fill(dest, destOffset, destOffset + length, (byte) 7),
                closeCount::incrementAndGet);

        assertEquals(0, closeCount.get(), "the resource must stay open while the stream is readable");
        assertArrayEquals(new byte[]{7, 7}, stream.readNBytes(2));

        stream.close();
        assertEquals(1, closeCount.get(), "close() must release the backing resource");

        stream.close();
        assertEquals(1, closeCount.get(), "a second close() must not release the resource again");
    }

    @Test
    void closeWithoutABackingResourceStillClosesTheStream() throws IOException {
        // The in-memory and request-per-range backends own no handle and pass null.
        RangeSeekableInputStream stream = createStream(new byte[4], 0, new ArrayList<>());

        stream.close();

        assertThrows(IOException.class, stream::read);
    }

    @Test
    void closedStreamRejectsOperations() throws IOException {
        RangeSeekableInputStream closedStream = createStream(new byte[10], 0, new ArrayList<>());
        closedStream.close();
        assertThrows(IOException.class, closedStream::read);
        assertThrows(IOException.class, () -> closedStream.seek(0));
        assertThrows(IOException.class, closedStream::position);
    }

    @Test
    void invalidSizesAndNegativeSeekAreRejected() throws IOException {
        assertThrows(IllegalArgumentException.class,
                () -> new RangeSeekableInputStream(-1, 0, (offset, dest, destOffset, length) -> {}));
        assertThrows(IllegalArgumentException.class,
                () -> new RangeSeekableInputStream(1, -1, (offset, dest, destOffset, length) -> {}));

        RangeSeekableInputStream stream = createStream(new byte[1], 0, new ArrayList<>());
        assertThrows(IOException.class, () -> stream.seek(-1));
    }

    private static RangeSeekableInputStream createStream(byte[] content, int readBufferSize, List<RangeCall> calls) {
        return new RangeSeekableInputStream(content.length, readBufferSize, (offset, dest, destOffset, length) -> {
            calls.add(new RangeCall(offset, length));
            System.arraycopy(content, Math.toIntExact(offset), dest, destOffset, length);
        });
    }
}
