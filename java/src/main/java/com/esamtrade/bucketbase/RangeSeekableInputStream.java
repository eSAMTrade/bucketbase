package com.esamtrade.bucketbase;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link SeekableInputStream} backed by a {@link RangeReader} that fetches byte ranges on
 * demand. Small reads are served from a single prefetched buffer; reads at least as large as
 * the buffer bypass it and are filled straight into the caller's array, so a large read never
 * allocates a second copy.
 *
 * <p>Not thread-safe on its own; all mutating methods are {@code synchronized} so a single
 * instance behaves correctly when shared, but callers should not interleave reads and seeks
 * from multiple threads expecting a coherent position.</p>
 */
final class RangeSeekableInputStream extends SeekableInputStream {

    /**
     * Fetches an exact byte range from the backing store into a caller-provided buffer. This is
     * the single extension point for a backend: it must fill exactly {@code length} bytes or
     * throw. Ranges are always within {@code [0, objectSize)}.
     */
    @FunctionalInterface
    interface RangeReader {
        void readInto(long offset, byte[] destination, int destinationOffset, int length) throws IOException;
    }

    private static final byte[] EMPTY_BUFFER = new byte[0];

    private final long objectSize;
    private final int readBufferSize;
    private final RangeReader rangeReader;

    private byte[] buffer = EMPTY_BUFFER;
    private long bufferOffset;
    private long position;
    private long markedPosition = -1;
    private boolean closed;

    RangeSeekableInputStream(long objectSize, int readBufferSize, RangeReader rangeReader) {
        if (objectSize < 0) {
            throw new IllegalArgumentException("objectSize must be non-negative");
        }
        if (readBufferSize < 0) {
            throw new IllegalArgumentException("readBufferSize must be non-negative");
        }
        this.objectSize = objectSize;
        this.readBufferSize = readBufferSize;
        this.rangeReader = Objects.requireNonNull(rangeReader, "rangeReader");
    }

    @Override
    public synchronized int read() throws IOException {
        byte[] one = new byte[1];
        return read(one, 0, 1) == -1 ? -1 : Byte.toUnsignedInt(one[0]);
    }

    @Override
    public synchronized int read(byte[] destination, int offset, int length) throws IOException {
        Objects.checkFromIndexSize(offset, length, destination.length);
        ensureOpen();
        if (length == 0) {
            return 0;
        }
        if (position >= objectSize) {
            return -1;
        }

        int toRead = (int) Math.min(length, objectSize - position);
        // Large read: fill the caller's array directly, no intermediate buffer.
        if (readBufferSize == 0 || toRead >= readBufferSize) {
            rangeReader.readInto(position, destination, offset, toRead);
            position += toRead;
            return toRead;
        }
        // Small read: serve from (and refill) the prefetch buffer.
        fillBufferAround(position);
        int bufferIndex = (int) (position - bufferOffset);
        int fromBuffer = Math.min(toRead, buffer.length - bufferIndex);
        System.arraycopy(buffer, bufferIndex, destination, offset, fromBuffer);
        position += fromBuffer;
        return fromBuffer;
    }

    @Override
    public synchronized byte[] readAllBytes() throws IOException {
        ensureOpen();
        long remaining = Math.max(0, objectSize - position);
        if (remaining == 0) {
            return EMPTY_BUFFER;
        }
        if (remaining > Integer.MAX_VALUE) {
            throw new IOException("Object is too large to return as one byte array: " + remaining + " bytes");
        }
        byte[] data = new byte[(int) remaining];
        rangeReader.readInto(position, data, 0, data.length);
        position += remaining;
        return data;
    }

    @Override
    public synchronized long skip(long count) throws IOException {
        ensureOpen();
        if (count <= 0) {
            return 0;
        }
        long skipped = Math.min(count, objectSize - Math.min(position, objectSize));
        position += skipped;
        return skipped;
    }

    /**
     * Bytes readable without a fetch: what remains in the prefetch buffer ahead of the current
     * position. Every other byte requires a blocking range read, so it is not reported here.
     */
    @Override
    public synchronized int available() throws IOException {
        ensureOpen();
        if (position >= bufferOffset && position < bufferOffset + buffer.length) {
            return (int) (bufferOffset + buffer.length - position);
        }
        return 0;
    }

    @Override
    public synchronized long position() throws IOException {
        ensureOpen();
        return position;
    }

    @Override
    public long size() {
        return objectSize;
    }

    @Override
    public synchronized void seek(long newPosition) throws IOException {
        ensureOpen();
        if (newPosition < 0) {
            throw new IOException("Cannot seek to a negative position: " + newPosition);
        }
        position = newPosition;
    }

    // mark/reset are cheap on a seekable stream: they just remember and restore the position.
    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readLimit) {
        markedPosition = position;
    }

    @Override
    public synchronized void reset() throws IOException {
        ensureOpen();
        if (markedPosition < 0) {
            throw new IOException("reset() called before mark()");
        }
        position = markedPosition;
    }

    @Override
    public synchronized void close() {
        closed = true;
        buffer = EMPTY_BUFFER;
    }

    private void fillBufferAround(long from) throws IOException {
        if (from >= bufferOffset && from < bufferOffset + buffer.length) {
            return;
        }
        int length = (int) Math.min(readBufferSize, objectSize - from);
        byte[] refilled = new byte[length];
        rangeReader.readInto(from, refilled, 0, length);
        buffer = refilled;
        bufferOffset = from;
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
    }
}
