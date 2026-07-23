package com.esamtrade.bucketbase;

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves that {@link IBucket#getObjectStream(PurePosixPath)} is a good enough random-access
 * stream to drive a real Parquet reader: footer-only reads, column projection and predicate
 * pushdown all translate into partial reads of the object rather than a full download.
 *
 * <p>This is the Java counterpart of the Python assertions in
 * {@code tests/test_minio_range_reader.py::test_parquet_tail_reads_selected_columns_without_full_object_get}
 * and {@code tests/bucket_tester.py::test_get_object_stream_with_parquet_tail}.</p>
 */
class ParquetReadTest {

    private static final int ROWS = 30_000;
    private static final PurePosixPath OBJECT = PurePosixPath.from("parquet/read-test.parquet");

    private MemoryBucket bucket;
    private long objectSize;

    @BeforeEach
    void setUp() throws IOException {
        bucket = new MemoryBucket();
        byte[] parquet = ParquetTestSupport.parquetBytes(ROWS);
        bucket.putObject(OBJECT, parquet);
        objectSize = parquet.length;
    }

    @Test
    @DisplayName("getSize reports the stored object size")
    void getSizeMatchesStoredObject() throws IOException {
        assertEquals(objectSize, bucket.getSize(OBJECT));
    }

    @Test
    @DisplayName("all rows and row groups round-trip through the bucket")
    void roundTripsEveryRow() throws IOException {
        assertEquals(ROWS, ParquetTestSupport.countAllRows(bucket, OBJECT));
    }

    @Test
    @DisplayName("reading the footer touches only the tail of the object")
    void footerReadIsPartial() throws IOException {
        ParquetTestSupport.ReadResult footer = ParquetTestSupport.readFooterOnly(bucket, OBJECT);

        assertEquals(ROWS / ParquetTestSupport.ROWS_PER_ROW_GROUP, footer.rowGroupsTotal(),
                "fixture should have one row group per " + ParquetTestSupport.ROWS_PER_ROW_GROUP + " rows");
        assertTrue(footer.bytesFetched() < objectSize / 10,
                "footer read fetched " + footer.bytesFetched() + " of " + objectSize + " bytes; expected under 10%");
    }

    @Test
    @DisplayName("column projection skips the unread columns' data")
    void projectionReadsLessThanFullScan() throws IOException {
        ParquetTestSupport.ReadResult projected = ParquetTestSupport.readIds(bucket, OBJECT, Optional.empty());

        assertEquals(ROWS, projected.ids().size(), "projection must still return every row");
        assertEquals(0L, projected.ids().get(0));
        assertEquals(ROWS - 1L, projected.ids().get(projected.ids().size() - 1));
        assertEquals(projected.rowGroupsTotal(), projected.rowGroupsRead(),
                "no predicate means no row group is pruned");
        assertTrue(projected.bytesFetched() < objectSize / 2,
                "projected read fetched " + projected.bytesFetched() + " of " + objectSize
                        + " bytes; the wide `name` column should not have been read");
    }

    @Test
    @DisplayName("predicate pushdown prunes row groups using column statistics")
    void predicatePushdownPrunesRowGroups() throws IOException {
        // ids run 0..29999 in three row groups of 10000; only the last can satisfy id > 25000.
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 25_000L)), false);
        ParquetTestSupport.ReadResult filtered = ParquetTestSupport.readIds(bucket, OBJECT, Optional.of(domain));
        ParquetTestSupport.ReadResult unfiltered = ParquetTestSupport.readIds(bucket, OBJECT, Optional.empty());

        assertEquals(1, filtered.rowGroupsRead(), "only the last row group can match id > 25000");
        assertEquals(3, filtered.rowGroupsTotal());

        List<Long> ids = filtered.ids();
        assertFalse(ids.isEmpty(), "the matching row group must still be read");
        // Row-group pruning is coarse: the reader returns whole row groups, so every returned id
        // comes from the surviving group rather than being exactly the predicate's result set.
        assertTrue(ids.stream().allMatch(id -> id >= 20_000),
                "pruned row groups must not contribute rows; got min id " + ids.stream().mapToLong(Long::longValue).min().orElseThrow());
        assertTrue(ids.contains(29_999L), "the surviving row group runs to the end of the file");

        assertTrue(filtered.bytesFetched() < unfiltered.bytesFetched(),
                "pushdown fetched " + filtered.bytesFetched() + " bytes, which should be less than the "
                        + unfiltered.bytesFetched() + " bytes of an unfiltered projection");
    }

    @Test
    @DisplayName("a predicate no row group can satisfy reads nothing but the footer")
    void unsatisfiablePredicateReadsOnlyFooter() throws IOException {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 10_000_000L)), false);
        ParquetTestSupport.ReadResult filtered = ParquetTestSupport.readIds(bucket, OBJECT, Optional.of(domain));
        ParquetTestSupport.ReadResult footerOnly = ParquetTestSupport.readFooterOnly(bucket, OBJECT);

        assertEquals(0, filtered.rowGroupsRead(), "every row group should be pruned");
        assertTrue(filtered.ids().isEmpty());
        assertEquals(footerOnly.bytesFetched(), filtered.bytesFetched(),
                "with all row groups pruned, only the footer should have been fetched");
    }

    @Test
    @DisplayName("the object stream seeks backwards and forwards without reopening")
    void streamSupportsRandomAccess() throws IOException {
        byte[] whole = bucket.getObject(OBJECT);
        try (SeekableInputStream stream = bucket.getObjectStream(OBJECT)) {
            assertEquals(objectSize, stream.size());

            stream.seek(objectSize - 4);
            byte[] magic = stream.readNBytes(4);
            assertEquals("PAR1", new String(magic, java.nio.charset.StandardCharsets.US_ASCII));
            assertEquals(objectSize, stream.position());

            // Seek backwards to the header - a fresh range read, no reopen.
            stream.seek(0);
            assertEquals("PAR1", new String(stream.readNBytes(4), java.nio.charset.StandardCharsets.US_ASCII));

            // A mid-object seek must agree with the fully downloaded bytes.
            long middle = objectSize / 2;
            stream.seek(middle);
            byte[] sample = stream.readNBytes(64);
            byte[] expected = java.util.Arrays.copyOfRange(whole, (int) middle, (int) middle + 64);
            org.junit.jupiter.api.Assertions.assertArrayEquals(expected, sample);
        }
    }
}
