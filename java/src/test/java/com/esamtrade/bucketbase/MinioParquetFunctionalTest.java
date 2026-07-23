package com.esamtrade.bucketbase;

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end Parquet tests against a real S3-compatible service.
 *
 * <p>These are the Java counterpart of the Python MinIO tests: they verify that ranged HTTP GETs
 * really do let a Parquet reader read a footer, project columns and prune row groups without
 * downloading the whole object, and that a streamed multipart write commits or aborts correctly.
 * The in-memory tests cannot catch range-header, ETag or multipart-sizing problems.</p>
 *
 * <p>The bucket is configured through {@link TestConfig}; each run uses a unique prefix and
 * cleans up after itself.</p>
 */
@EnabledIf("com.esamtrade.bucketbase.TestConfig#functionalTestsEnabled")
class MinioParquetFunctionalTest {

    private static final int ROWS = 30_000;

    private S3Bucket bucket;
    private PurePosixPath prefix;
    private PurePosixPath object;

    @BeforeEach
    void setUp() throws IOException {
        // readBufferSize 0 disables client-side read-ahead so the byte counts below are an exact
        // account of what was pulled over the wire.
        bucket = new S3Bucket(
                TestConfig.MINIO_ENDPOINT,
                TestConfig.MINIO_ACCESS_KEY,
                TestConfig.MINIO_SECRET_KEY,
                TestConfig.MINIO_BUCKET,
                0);
        prefix = PurePosixPath.from("bucketbase-java-parquet-" + UUID.randomUUID().toString().substring(0, 8) + "/");
        object = prefix.join("data.parquet");
    }

    @AfterEach
    void tearDown() throws IOException {
        bucket.removePrefix(prefix);
    }

    @Test
    @DisplayName("a Parquet file streamed through openWrite round-trips via real multipart upload")
    void streamedWriteRoundTrips() throws IOException {
        try (ObjectWriter writer = bucket.openWrite(object)) {
            ParquetTestSupport.writeParquet(writer.stream(), ROWS);
            writer.commit();
        }

        assertTrue(bucket.exists(object));
        assertTrue(bucket.getSize(object) > 0);
        assertEquals(ROWS, ParquetTestSupport.countAllRows(bucket, object));
    }

    @Test
    @DisplayName("footer, projection and pushdown each read only part of the remote object")
    void partialReadsOverHttpRange() throws IOException {
        bucket.putObject(object, ParquetTestSupport.parquetBytes(ROWS));
        long objectSize = bucket.getSize(object);

        ParquetTestSupport.ReadResult footer = ParquetTestSupport.readFooterOnly(bucket, object);
        ParquetTestSupport.ReadResult projected = ParquetTestSupport.readIds(bucket, object, Optional.empty());
        ParquetTestSupport.ReadResult pushedDown = ParquetTestSupport.readIds(bucket, object,
                Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 25_000L)), false)));

        // Footer-only: the reader seeks to the tail rather than downloading the object.
        assertTrue(footer.bytesFetched() < objectSize / 10,
                "footer read pulled " + footer.bytesFetched() + " of " + objectSize + " bytes");

        // Column projection: the wide `name` column is never fetched.
        assertEquals(ROWS, projected.ids().size());
        assertTrue(projected.bytesFetched() < objectSize,
                "projected read pulled " + projected.bytesFetched() + " of " + objectSize + " bytes");

        // Predicate pushdown: two of three row groups are pruned from statistics alone.
        assertEquals(1, pushedDown.rowGroupsRead());
        assertEquals(3, pushedDown.rowGroupsTotal());
        assertTrue(pushedDown.bytesFetched() < projected.bytesFetched(),
                "pushdown pulled " + pushedDown.bytesFetched() + " bytes vs " + projected.bytesFetched()
                        + " for the unfiltered projection");
        assertTrue(pushedDown.ids().stream().allMatch(id -> id >= 20_000));

        // Several distinct ranged GETs, not one big download.
        assertTrue(projected.rangeRequests() > 1,
                "expected multiple ranged reads, got " + projected.rangeRequests());
    }

    @Test
    @DisplayName("an unsatisfiable predicate reads nothing but the footer from the remote object")
    void unsatisfiablePredicateReadsOnlyFooter() throws IOException {
        bucket.putObject(object, ParquetTestSupport.parquetBytes(ROWS));

        ParquetTestSupport.ReadResult footer = ParquetTestSupport.readFooterOnly(bucket, object);
        ParquetTestSupport.ReadResult filtered = ParquetTestSupport.readIds(bucket, object,
                Optional.of(Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 10_000_000L)), false)));

        assertEquals(0, filtered.rowGroupsRead());
        assertTrue(filtered.ids().isEmpty());
        assertEquals(footer.bytesFetched(), filtered.bytesFetched());
    }

    @Test
    @DisplayName("an abandoned streamed write leaves no object in the bucket")
    void abortedWriteLeavesNoObject() throws IOException {
        assertThrows(RuntimeException.class, () -> {
            try (ObjectWriter writer = bucket.openWrite(object)) {
                ParquetTestSupport.writeParquet(writer.stream(), ROWS);
                throw new RuntimeException("simulated failure before commit");
            }
        });

        assertFalse(bucket.exists(object), "the multipart upload should have been aborted");
        List<PurePosixPath> leftovers = bucket.listObjects(prefix);
        assertTrue(leftovers.isEmpty(), "no partial object should be listed, found: " + leftovers);
    }

    @Test
    @DisplayName("a body larger than the multipart part size uploads correctly from a chunked source")
    void multiPartUploadFromChunkedSource() throws IOException {
        // Larger than the 5 MiB part size, written in small chunks. The sink delivers whatever
        // the caller wrote, so the uploader must coalesce short reads into full-size parts -
        // otherwise S3 rejects the undersized non-final parts with EntityTooSmall.
        final int chunk = 8 * 1024;
        final int chunks = 1_400;                 // ~11 MiB => 3 parts
        byte[] payload = new byte[chunk];
        for (int i = 0; i < chunk; i++) {
            payload[i] = (byte) (i % 251);
        }

        try (ObjectWriter writer = bucket.openWrite(object)) {
            OutputStream sink = writer.stream();
            for (int i = 0; i < chunks; i++) {
                sink.write(payload);
            }
            writer.commit();
        }

        long expectedSize = (long) chunk * chunks;
        assertEquals(expectedSize, bucket.getSize(object));

        // Spot-check the bytes at a part boundary rather than downloading 11 MiB.
        try (SeekableInputStream stream = bucket.getObjectStream(object)) {
            long boundary = 5L * 1024 * 1024;
            stream.seek(boundary);
            byte[] sample = stream.readNBytes(16);
            for (int i = 0; i < sample.length; i++) {
                int offsetInChunk = (int) ((boundary + i) % chunk);
                assertEquals((byte) (offsetInChunk % 251), sample[i], "byte mismatch at offset " + (boundary + i));
            }
        }
    }

    @Test
    @DisplayName("putObjectStream can be called more than once on the same bucket instance")
    void putObjectStreamIsReusable() throws IOException {
        PurePosixPath first = prefix.join("first.bin");
        PurePosixPath second = prefix.join("second.bin");

        bucket.putObjectStream(first, new java.io.ByteArrayInputStream("first payload".getBytes()));
        bucket.putObjectStream(second, new java.io.ByteArrayInputStream("second payload".getBytes()));

        assertEquals("first payload", new String(bucket.getObject(first)));
        assertEquals("second payload", new String(bucket.getObject(second)));
    }
}
