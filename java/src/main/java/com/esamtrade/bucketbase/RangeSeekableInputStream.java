package com.esamtrade.bucketbase;

import java.io.IOException;
import java.util.Objects;

final class RangeSeekableInputStream extends SeekableInputStream {
    @FunctionalInterface
    interface RangeReader {
        byte[] read(long offset, int length) throws IOException;
    }

    private static final byte[] EMPTY_BUFFER = new byte[0];

    private final long objectSize;
    private final int readBufferSize;
    private final RangeReader rangeReader;
    private byte[] buffer = EMPTY_BUFFER;
    private long bufferOffset;
    private long position;
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
        byte[] result = new byte[1];
        return read(result, 0, 1) == -1 ? -1 : Byte.toUnsignedInt(result[0]);
    }

    @Override
    public synchronized int read(byte[] destination, int offset, int length) throws IOException {
        ensureOpen();
        Objects.checkFromIndexSize(offset, length, destination.length);
        if (length == 0) {
            return 0;
        }
        if (position >= objectSize) {
            return -1;
        }

        int requestedLength = (int) Math.min(length, objectSize - position);
        if (readBufferSize == 0 || requestedLength >= readBufferSize) {
            byte[] data = readRange(position, requestedLength);
            System.arraycopy(data, 0, destination, offset, requestedLength);
            position += requestedLength;
            return requestedLength;
        }

        int copied = 0;
        while (copied < requestedLength) {
            ensureBufferedPosition();
            int bufferIndex = Math.toIntExact(position - bufferOffset);
            int copyLength = Math.min(requestedLength - copied, buffer.length - bufferIndex);
            System.arraycopy(buffer, bufferIndex, destination, offset + copied, copyLength);
            copied += copyLength;
            position += copyLength;
        }
        return copied;
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
        byte[] data = readRange(position, (int) remaining);
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

    @Override
    public synchronized int available() throws IOException {
        ensureOpen();
        return (int) Math.min(Integer.MAX_VALUE, Math.max(0, objectSize - position));
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

    @Override
    public synchronized void close() {
        closed = true;
        buffer = EMPTY_BUFFER;
    }

    private void ensureBufferedPosition() throws IOException {
        if (position >= bufferOffset && position < bufferOffset + buffer.length) {
            return;
        }
        int length = (int) Math.min(readBufferSize, objectSize - position);
        bufferOffset = position;
        buffer = readRange(position, length);
    }

    private byte[] readRange(long offset, int length) throws IOException {
        byte[] data = rangeReader.read(offset, length);
        if (data.length != length) {
            throw new IOException("Expected " + length + " bytes at offset " + offset + ", but received " + data.length);
        }
        return data;
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
    }
}
