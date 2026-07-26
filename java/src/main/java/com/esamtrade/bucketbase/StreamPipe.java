package com.esamtrade.bucketbase;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Objects;

/**
 * A bounded single-producer/single-consumer byte pipe bridging an {@link OutputStream} written by
 * the caller to an {@link InputStream} read by a background uploader.
 *
 * <p>Unlike {@link java.io.PipedInputStream} it can make the reader <em>fail</em> on demand
 * ({@link #abort}), which is what lets an aborted write make the in-flight upload throw instead of
 * committing a truncated object. A slow reader back-pressures the writer rather than letting data
 * accumulate: at most {@value #MAX_QUEUED_CHUNKS} chunks of {@value #MAX_CHUNK_BYTES} bytes are
 * buffered, and a larger write blocks until the reader drains, however big a single
 * {@code write(byte[])} call is.</p>
 *
 * <p>Coordination is a single monitor with blocking {@code wait}/{@code notifyAll} — no timed
 * polling.</p>
 */
final class StreamPipe {

    /** Largest chunk enqueued at once; caps the copy made per write and bounds queued bytes. */
    private static final int MAX_CHUNK_BYTES = 1 << 20;
    /** Chunks buffered before the writer blocks: capacity is MAX_QUEUED_CHUNKS * MAX_CHUNK_BYTES. */
    private static final int MAX_QUEUED_CHUNKS = 2;
    private static final byte[] EMPTY = new byte[0];

    private final Object lock = new Object();
    private final Deque<byte[]> queue = new ArrayDeque<>();
    private boolean writerFinished;   // finish() was called: EOF once the queue drains
    private boolean readerClosed;     // the reader stopped consuming
    private Throwable failure;        // abort() cause

    private final InputStream input = new PipeInputStream();
    private final OutputStream output = new PipeOutputStream();

    InputStream inputStream() {
        return input;
    }

    OutputStream outputStream() {
        return output;
    }

    /** Clean end of stream: the reader sees EOF and the upload commits normally. */
    void finish() {
        synchronized (lock) {
            writerFinished = true;
            lock.notifyAll();
        }
    }

    /** Makes the reader fail with {@code cause}; any queued-but-unsent data is discarded. */
    void abort(Throwable cause) {
        synchronized (lock) {
            if (failure == null) {
                failure = cause;
            }
            queue.clear();
            lock.notifyAll();
        }
    }

    private void awaitOn() throws IOException {
        try {
            lock.wait();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while streaming the object body", e);
        }
    }

    /** Consumer side, handed to {@code putObjectStream} on the uploader thread. */
    private final class PipeInputStream extends InputStream {
        private byte[] chunk = EMPTY;
        private int pos;

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            return read(one, 0, 1) == -1 ? -1 : Byte.toUnsignedInt(one[0]);
        }

        @Override
        public int read(byte[] dest, int off, int len) throws IOException {
            Objects.checkFromIndexSize(off, len, dest.length);
            if (len == 0) {
                return 0;
            }
            if (pos >= chunk.length && !nextChunk()) {
                return -1;
            }
            int n = Math.min(len, chunk.length - pos);
            System.arraycopy(chunk, pos, dest, off, n);
            pos += n;
            return n;
        }

        /** Blocks until a chunk is available; returns false at clean EOF; throws on abort. */
        private boolean nextChunk() throws IOException {
            synchronized (lock) {
                while (queue.isEmpty() && !writerFinished && failure == null) {
                    awaitOn();
                }
                if (failure != null) {
                    throw new IOException("Object write was aborted before completion", failure);
                }
                if (queue.isEmpty()) {   // writerFinished with nothing left
                    return false;
                }
                chunk = queue.removeFirst();
                pos = 0;
                lock.notifyAll();        // a slot freed up for the writer
                return true;
            }
        }

        @Override
        public void close() {
            synchronized (lock) {
                readerClosed = true;
                lock.notifyAll();        // wake a writer blocked on back-pressure
            }
        }
    }

    /** Producer side, handed to the caller. */
    private final class PipeOutputStream extends OutputStream {
        @Override
        public void write(int b) throws IOException {
            write(new byte[]{(byte) b}, 0, 1);
        }

        @Override
        public void write(byte[] src, int off, int len) throws IOException {
            Objects.checkFromIndexSize(off, len, src.length);
            // Split into bounded chunks so capacity is bounded by bytes buffered, not by the
            // number of write() calls: one huge write must be back-pressured like many small ones.
            int written = 0;
            while (written < len) {
                int size = Math.min(MAX_CHUNK_BYTES, len - written);
                // Copy: the caller may reuse its buffer as soon as write() returns.
                byte[] copy = Arrays.copyOfRange(src, off + written, off + written + size);
                synchronized (lock) {
                    while (queue.size() >= MAX_QUEUED_CHUNKS && !readerClosed && failure == null) {
                        awaitOn();
                    }
                    if (failure != null || readerClosed) {
                        throw new IOException("Cannot write: the object stream is closed");
                    }
                    queue.addLast(copy);
                    lock.notifyAll();
                }
                written += size;
            }
        }
    }
}
