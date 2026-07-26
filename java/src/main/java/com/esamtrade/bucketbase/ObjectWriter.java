package com.esamtrade.bucketbase;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A streaming sink for a single object. Bytes written to {@link #stream()} are uploaded by a
 * background thread as they arrive, so an arbitrarily large object can be produced without
 * buffering it in memory. For S3-backed buckets this maps onto a multipart upload.
 *
 * <p><strong>Nothing is stored unless {@link #commit()} is called.</strong> Closing without
 * committing aborts the upload and leaves no object behind. This is the guarantee that makes
 * the writer safe to use with formats that finalise on close, such as Parquet:</p>
 *
 * <pre>{@code
 * try (ObjectWriter writer = bucket.openWrite(PurePosixPath.from("data.parquet"))) {
 *     try (ParquetWriter parquet = new ParquetWriter(writer.stream(), ...)) {
 *         parquet.write(page);
 *     }            // writes the Parquet footer and closes the stream - but does NOT commit
 *     writer.commit();
 * }                // on an exception the upload is aborted; no partial object is visible
 * }</pre>
 *
 * <p>{@link #stream()} deliberately ignores {@link OutputStream#close()} - a nested writer
 * closing the sink must not be able to publish the object. Only {@code commit()} does that.
 * This mirrors the Python library's {@code NonClosingStream} wrapper inside
 * {@code AsyncObjectWriter}.</p>
 *
 * <p>Instances are not thread-safe: write from one thread at a time.</p>
 */
public final class ObjectWriter implements AutoCloseable {

    /** How long {@link #commit()} and {@link #close()} wait for the uploader to settle. */
    public static final long DEFAULT_TIMEOUT_MILLIS = 5 * 60 * 1000L;

    /** How long a cancelled uploader is given to unwind after being interrupted. */
    private static final long CANCELLATION_GRACE_MILLIS = 2_000L;

    private final PurePosixPath name;
    private final StreamPipe pipe = new StreamPipe();
    private final AtomicReference<Throwable> uploadFailure = new AtomicReference<>();
    private final Thread uploader;
    private final long timeoutMillis;
    private final OutputStream sink;

    private boolean committed;
    private boolean closed;

    ObjectWriter(IBucket bucket, PurePosixPath name, long timeoutMillis) {
        this.name = Objects.requireNonNull(name, "name");
        Objects.requireNonNull(bucket, "bucket");
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeoutMillis must be positive");
        }
        this.timeoutMillis = timeoutMillis;
        this.sink = new NonClosingOutputStream(pipe.outputStream());
        this.uploader = new Thread(() -> {
            // Closing the pipe input on exit wakes a writer blocked on back-pressure if the
            // upload ends early (e.g. the store rejected it), so the caller cannot hang.
            try (java.io.InputStream in = pipe.inputStream()) {
                bucket.putObjectStream(name, in);
            } catch (Throwable t) {
                uploadFailure.set(t);
            }
        }, "bucketbase-writer-" + name);
        this.uploader.setDaemon(true);
        this.uploader.start();
    }

    /**
     * The sink to write the object body to. Closing it has no effect; see the class javadoc.
     */
    public OutputStream stream() {
        return sink;
    }

    public PurePosixPath name() {
        return name;
    }

    /**
     * Finishes the upload and makes the object visible. After this returns successfully the
     * object is stored.
     *
     * @throws IOException if the upload failed, timed out, or the writer was already closed
     */
    public void commit() throws IOException {
        if (closed) {
            throw new IOException("Cannot commit " + name + ": the writer is already closed");
        }
        if (committed) {
            return;
        }
        pipe.finish();
        awaitUploader();
        Throwable failure = uploadFailure.get();
        if (failure != null) {
            throw asIOException("Failed to write object " + name, failure);
        }
        committed = true;
    }

    /**
     * Aborts the upload unless {@link #commit()} succeeded first. Safe to call repeatedly, and
     * safe to call after {@code commit()} - in that case it is a no-op.
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (committed) {
            return;
        }
        pipe.abort(new IOException("Object write for " + name + " was abandoned without commit"));
        awaitUploader();
        // A failure here is the expected consequence of the abort, so it is not propagated:
        // the contract is simply that no object was created.
        uploadFailure.set(null);
    }

    /**
     * Waits for the uploader to settle, cancelling it if it outlives the timeout.
     *
     * <p>The timeout has to be an upper bound on the <em>upload</em>, not merely on the caller's
     * wait: an uploader left running could finish later and publish an object the caller was
     * already told had failed. Cancellation is best-effort - it depends on the underlying store
     * honouring interruption - but leaving the thread running guarantees the contract is broken.</p>
     */
    private void awaitUploader() throws IOException {
        boolean timedOut = false;
        try {
            uploader.join(timeoutMillis);
            if (uploader.isAlive()) {
                timedOut = true;
                uploader.interrupt();
                // Give the interrupt a brief chance to unwind before reporting the timeout.
                uploader.join(CANCELLATION_GRACE_MILLIS);
            }
        } catch (InterruptedException e) {
            uploader.interrupt();
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while finishing the write of " + name, e);
        }
        if (uploader.isAlive()) {
            throw new IOException("Timed out after " + timeoutMillis + " ms waiting for the upload of " + name
                    + "; the upload was cancelled but did not stop");
        }
        if (timedOut) {
            throw new IOException("Timed out after " + timeoutMillis + " ms waiting for the upload of " + name);
        }
    }

    private static IOException asIOException(String message, Throwable cause) {
        if (cause instanceof IOException io) {
            return new IOException(message + ": " + io.getMessage(), io);
        }
        return new IOException(message, cause);
    }

    /**
     * Wraps the pipe so that a nested writer's {@code close()} cannot commit the object.
     */
    private static final class NonClosingOutputStream extends OutputStream {
        private final OutputStream delegate;

        NonClosingOutputStream(OutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
        }

        @Override
        public void write(byte[] source) throws IOException {
            delegate.write(source);
        }

        @Override
        public void write(byte[] source, int offset, int length) throws IOException {
            delegate.write(source, offset, length);
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() {
            // Intentionally not closing: only ObjectWriter.commit() may end the upload.
        }
    }
}
