package com.esamtrade.bucketbase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers {@link IBucket#openWrite(PurePosixPath)} with a real Parquet writer.
 *
 * <p>Parquet finalises a file by writing its footer when the writer is closed, so a naive sink
 * would publish the object at exactly the wrong moment. These tests pin down the commit/abort
 * contract that makes that safe, mirroring the Python regression tests
 * {@code test_regression_exception_thrown_in_parquet_writer_context_doesnt_save_object} and
 * friends in {@code python/tests/bucket_tester.py}.</p>
 */
class ParquetWriteTest {

    private static final int ROWS = 25_000;
    private static final PurePosixPath OBJECT = PurePosixPath.from("parquet/write-test.parquet");

    private MemoryBucket bucket;

    @BeforeEach
    void setUp() {
        bucket = new MemoryBucket();
    }

    @Test
    @DisplayName("a streamed Parquet file commits and reads back intact")
    void streamingWriteRoundTrips() throws IOException {
        try (ObjectWriter writer = bucket.openWrite(OBJECT)) {
            ParquetTestSupport.writeParquet(writer.stream(), ROWS);
            writer.commit();
        }

        assertTrue(bucket.exists(OBJECT));
        assertEquals(ROWS, ParquetTestSupport.countAllRows(bucket, OBJECT));
        assertTrue(bucket.getSize(OBJECT) > 0);
    }

    @Test
    @DisplayName("the object is invisible until commit, even after the Parquet footer is written")
    void nestedCloseDoesNotCommit() throws IOException {
        try (ObjectWriter writer = bucket.openWrite(OBJECT)) {
            // writeParquet closes the ParquetWriter internally, which emits the footer and
            // closes the sink. That must not publish the object.
            ParquetTestSupport.writeParquet(writer.stream(), 1_000);
            assertFalse(bucket.exists(OBJECT),
                    "object must not exist after the Parquet writer closed the sink but before commit()");

            writer.commit();
            assertTrue(bucket.exists(OBJECT), "commit() must publish the object");
        }
    }

    @Test
    @DisplayName("an exception inside the writer context leaves no object behind")
    void abandonedWriteLeavesNoObject() {
        RuntimeException boom = assertThrows(RuntimeException.class, () -> {
            try (ObjectWriter writer = bucket.openWrite(OBJECT)) {
                ParquetTestSupport.writeParquet(writer.stream(), 5_000);
                throw new RuntimeException("simulated failure after writing the Parquet body");
            }
        });

        assertEquals("simulated failure after writing the Parquet body", boom.getMessage());
        assertFalse(bucket.exists(OBJECT), "an aborted write must not create an object");
        assertTrue(bucket.listObjects(PurePosixPrefix.from("parquet/")).isEmpty(),
                "no partial object should be listed either");
    }

    @Test
    @DisplayName("failing mid-stream, before any footer, also leaves no object")
    void failureMidStreamLeavesNoObject() {
        assertThrows(IllegalStateException.class, () -> {
            try (ObjectWriter writer = bucket.openWrite(OBJECT)) {
                OutputStream sink = writer.stream();
                sink.write(new byte[64 * 1024]);
                throw new IllegalStateException("aborted halfway");
            }
        });

        assertFalse(bucket.exists(OBJECT));
    }

    @Test
    @DisplayName("closing without committing is not an error, it just discards the write")
    void closeWithoutCommitIsSilent() throws IOException {
        try (ObjectWriter writer = bucket.openWrite(OBJECT)) {
            writer.stream().write("some bytes that will never be stored".getBytes());
        }
        assertFalse(bucket.exists(OBJECT));
    }

    @Test
    @DisplayName("committing an empty body stores an empty object")
    void emptyWriteCommits() throws IOException {
        try (ObjectWriter writer = bucket.openWrite(OBJECT)) {
            writer.commit();
        }
        assertTrue(bucket.exists(OBJECT));
        assertEquals(0, bucket.getSize(OBJECT));
    }

    @Test
    @DisplayName("commit after close is rejected")
    void commitAfterCloseFails() throws IOException {
        ObjectWriter writer = bucket.openWrite(OBJECT);
        writer.stream().write("data".getBytes());
        writer.close();

        IOException failure = assertThrows(IOException.class, writer::commit);
        assertTrue(failure.getMessage().contains("already closed"), failure.getMessage());
        assertFalse(bucket.exists(OBJECT));
    }

    @Test
    @DisplayName("commit is idempotent and close after commit is a no-op")
    void commitIsIdempotent() throws IOException {
        try (ObjectWriter writer = bucket.openWrite(OBJECT)) {
            writer.stream().write("payload".getBytes());
            writer.commit();
            writer.commit();
        }
        assertEquals("payload", new String(bucket.getObject(OBJECT)));
    }

    @Test
    @DisplayName("many small writes stream through without buffering the whole body")
    void manySmallWritesStream() throws IOException {
        try (ObjectWriter writer = bucket.openWrite(OBJECT)) {
            OutputStream sink = writer.stream();
            for (int i = 0; i < 5_000; i++) {
                sink.write(("line-" + i + "\n").getBytes());
            }
            writer.commit();
        }

        String stored = new String(bucket.getObject(OBJECT));
        assertTrue(stored.startsWith("line-0\n"));
        assertTrue(stored.endsWith("line-4999\n"));
        assertEquals(5_000, stored.lines().count());
    }
}
