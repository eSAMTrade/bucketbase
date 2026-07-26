package com.esamtrade.bucketbase;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ObjectWriter} promises that an object is stored only after {@link ObjectWriter#commit()}
 * returns successfully, and that an abandoned write leaves nothing behind.
 *
 * <p>Bounding only the caller's wait is not enough to honour that: if the uploader thread is left
 * running after a timeout or an abort, it can finish the upload later and publish an object the
 * caller was already told had failed. Cancellation is best-effort - it depends on the underlying
 * store honouring interruption - but the writer must at least request it.</p>
 */
class ObjectWriterCancellationTest {

    private static final PurePosixPath OBJECT = PurePosixPath.from("dir/slow.bin");

    @Test
    void aTimedOutCommitCancelsTheUploader() throws Exception {
        SlowBucket bucket = new SlowBucket(1_500);

        IOException failure = assertThrows(IOException.class, () -> {
            try (ObjectWriter writer = bucket.openWrite(OBJECT, 200)) {
                writer.stream().write("payload".getBytes());
                writer.commit();
            }
        });
        assertTrue(failure.getMessage().contains("Timed out"), failure.getMessage());

        assertFalse(bucket.stored.await(3, TimeUnit.SECONDS),
                "the uploader kept running after the timeout and stored the object anyway");
        assertFalse(bucket.exists(OBJECT), "no object may exist after a failed commit");
    }

    @Test
    void anAbandonedWriteCancelsTheUploader() throws Exception {
        SlowBucket bucket = new SlowBucket(1_500);

        try (ObjectWriter writer = bucket.openWrite(OBJECT, 200)) {
            writer.stream().write("payload".getBytes());
            // No commit: closing must abort, and must not leave the uploader running.
        } catch (IOException expectedOnAbort) {
            // close() may surface a timeout while cancelling; the object must still be absent.
        }

        assertFalse(bucket.stored.await(3, TimeUnit.SECONDS),
                "the uploader kept running after the write was abandoned and stored the object anyway");
        assertFalse(bucket.exists(OBJECT));
    }

    /** Drains the body, then stalls before storing, so a cancellation window exists. */
    private static final class SlowBucket extends MemoryBucket {
        private final CountDownLatch stored = new CountDownLatch(1);
        private final long stallMillis;

        SlowBucket(long stallMillis) {
            this.stallMillis = stallMillis;
        }

        @Override
        public void putObjectStream(PurePosixPath name, InputStream stream) throws IOException {
            byte[] body = stream.readAllBytes();
            try {
                Thread.sleep(stallMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("upload cancelled", e);
            }
            putObject(name, body);
            stored.countDown();
        }
    }
}
