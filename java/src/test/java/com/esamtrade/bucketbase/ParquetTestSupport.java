package com.esamtrade.bucketbase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.parquet.Column;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.ParquetTypeUtils;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.PredicateUtils;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * Shared Parquet fixtures and read helpers built on Trino's Hadoop-free Parquet library.
 *
 * <p>The fixture schema is {@code (id BIGINT, name VARCHAR, active BOOLEAN)} with sequential
 * ids, deliberately mirroring the Python test fixture in {@code python/tests/bucket_tester.py}
 * so both languages assert the same shape of data.</p>
 */
public final class ParquetTestSupport {

    public static final List<String> COLUMN_NAMES = ImmutableList.of("id", "name", "active");
    public static final List<Type> COLUMN_TYPES = ImmutableList.of(BIGINT, VARCHAR, BOOLEAN);

    /** Rows per row group in the fixtures, small enough to make pruning observable. */
    public static final int ROWS_PER_ROW_GROUP = 10_000;

    private ParquetTestSupport() {
    }

    /**
     * Reader options tuned for byte accounting: a small footer read and no range merging, so a
     * measured byte count reflects the data actually needed rather than Trino's read-ahead
     * heuristics. Production defaults are 48 KB footer reads and 1 MB merge distance.
     */
    public static ParquetReaderOptions measurableOptions() {
        return ParquetReaderOptions.builder()
                .withFooterReadSize(DataSize.ofBytes(8192))
                .withMaxMergeDistance(DataSize.ofBytes(0))
                .build();
    }

    /**
     * Writes a Parquet file to an arbitrary {@link OutputStream}. The footer is emitted when the
     * underlying {@link io.trino.parquet.writer.ParquetWriter} is closed, which is precisely the
     * behaviour that makes {@link ObjectWriter}'s close/commit split necessary.
     */
    public static void writeParquet(OutputStream out, int rows) throws IOException {
        ParquetSchemaConverter schema = new ParquetSchemaConverter(COLUMN_TYPES, COLUMN_NAMES, false, false);
        ParquetWriterOptions options = ParquetWriterOptions.builder()
                .setMaxRowGroupRowCount(ROWS_PER_ROW_GROUP)
                .setMaxPageSize(DataSize.ofBytes(16 * 1024))
                .build();

        try (io.trino.parquet.writer.ParquetWriter writer = new io.trino.parquet.writer.ParquetWriter(
                out,
                schema.getMessageType(),
                schema.getPrimitiveTypes(),
                options,
                CompressionCodec.SNAPPY,
                "bucketbase-test",
                Optional.of(DateTimeZone.UTC),
                Optional.empty())) {
            // One page per row group keeps row-group boundaries deterministic.
            for (int start = 0; start < rows; start += ROWS_PER_ROW_GROUP) {
                int count = Math.min(ROWS_PER_ROW_GROUP, rows - start);
                writer.write(buildPage(start, count));
            }
        }
    }

    /** Builds the fixture bytes in memory. */
    public static byte[] parquetBytes(int rows) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeParquet(out, rows);
        return out.toByteArray();
    }

    private static Page buildPage(int firstId, int count) {
        BlockBuilder ids = BIGINT.createFixedSizeBlockBuilder(count);
        BlockBuilder names = VARCHAR.createBlockBuilder(null, count);
        BlockBuilder active = BOOLEAN.createFixedSizeBlockBuilder(count);
        for (int i = 0; i < count; i++) {
            long id = firstId + i;
            BIGINT.writeLong(ids, id);
            // Padded so the VARCHAR column is by far the largest, making projection measurable.
            VARCHAR.writeSlice(names, Slices.utf8Slice("object-name-with-padding-to-make-it-wide-" + id));
            BOOLEAN.writeBoolean(active, id % 2 == 0);
        }
        return new Page(ids.build(), names.build(), active.build());
    }

    /** Result of a projected/filtered read, with the byte accounting that proves partial reads. */
    public record ReadResult(List<Long> ids, int rowGroupsRead, int rowGroupsTotal, long bytesFetched, int rangeRequests) {
    }

    /** Reads only the footer, returning how many bytes that cost. */
    public static ReadResult readFooterOnly(IBucket bucket, PurePosixPath name) throws IOException {
        ParquetReaderOptions options = measurableOptions();
        try (BucketParquetDataSource dataSource = BucketParquetDataSource.open(bucket, name, options)) {
            ParquetMetadata metadata = MetadataReader.readFooter(dataSource, options, Optional.empty(), Optional.empty());
            return new ReadResult(
                    List.of(),
                    0,
                    metadata.getBlocks().size(),
                    dataSource.bytesFetched(),
                    dataSource.rangeRequests());
        }
    }

    /**
     * Reads the {@code id} column only, optionally pushing a predicate down so row groups whose
     * statistics cannot match are never fetched.
     *
     * @param idPredicate a domain over the {@code id} column, or empty for no pushdown
     */
    public static ReadResult readIds(IBucket bucket, PurePosixPath name, Optional<Domain> idPredicate) throws IOException {
        ParquetReaderOptions options = measurableOptions();
        try (BucketParquetDataSource dataSource = BucketParquetDataSource.open(bucket, name, options)) {
            ParquetMetadata metadata = MetadataReader.readFooter(dataSource, options, Optional.empty(), Optional.empty());
            FileMetadata fileMetadata = metadata.getFileMetaData();
            MessageType fileSchema = fileMetadata.getSchema();
            MessageColumnIO columnIO = ParquetTypeUtils.getColumnIO(fileSchema, fileSchema);

            // Column projection: only the columns we build a Column for are planned and fetched.
            List<Column> projected = ImmutableList.of(new Column(
                    "id",
                    ParquetTypeUtils.constructField(BIGINT, ParquetTypeUtils.lookupColumnByName(columnIO, "id")).orElseThrow()));

            Map<List<String>, ColumnDescriptor> descriptorsByPath = ParquetTypeUtils.getDescriptors(fileSchema, fileSchema);
            ColumnDescriptor idDescriptor = descriptorsByPath.get(ImmutableList.of("id"));

            TupleDomain<ColumnDescriptor> tupleDomain = idPredicate
                    .map(domain -> TupleDomain.withColumnDomains(ImmutableMap.of(idDescriptor, domain)))
                    .orElseGet(TupleDomain::all);
            TupleDomainParquetPredicate predicate =
                    PredicateUtils.buildPredicate(fileSchema, tupleDomain, descriptorsByPath, DateTimeZone.UTC);

            // Row-group pruning is the caller's job in Trino's API; ParquetReader only does
            // page-level filtering on the row groups it is handed.
            List<RowGroupInfo> rowGroups = PredicateUtils.getFilteredRowGroups(
                    0,
                    dataSource.getEstimatedSize(),
                    dataSource,
                    metadata,
                    ImmutableList.of(tupleDomain),
                    ImmutableList.of(predicate),
                    descriptorsByPath,
                    DateTimeZone.UTC,
                    1000,
                    options);

            List<Long> ids = new ArrayList<>();
            try (ParquetReader reader = new ParquetReader(
                    Optional.ofNullable(fileMetadata.getCreatedBy()),
                    projected,
                    false,
                    rowGroups,
                    dataSource,
                    DateTimeZone.UTC,
                    newSimpleAggregatedMemoryContext(),
                    options,
                    e -> {
                        throwIfUnchecked(e);
                        return new RuntimeException(e);
                    },
                    Optional.of(predicate),
                    Optional.empty(),
                    Optional.empty())) {
                SourcePage page;
                while ((page = reader.nextPage()) != null) {
                    Block block = page.getBlock(0);
                    for (int i = 0; i < block.getPositionCount(); i++) {
                        ids.add(BIGINT.getLong(block, i));
                    }
                }
            }
            return new ReadResult(
                    ids,
                    rowGroups.size(),
                    metadata.getBlocks().size(),
                    dataSource.bytesFetched(),
                    dataSource.rangeRequests());
        }
    }

    /** Reads every column of every row, for round-trip verification. */
    public static int countAllRows(IBucket bucket, PurePosixPath name) throws IOException {
        ParquetReaderOptions options = measurableOptions();
        try (BucketParquetDataSource dataSource = BucketParquetDataSource.open(bucket, name, options)) {
            ParquetMetadata metadata = MetadataReader.readFooter(dataSource, options, Optional.empty(), Optional.empty());
            FileMetadata fileMetadata = metadata.getFileMetaData();
            MessageType fileSchema = fileMetadata.getSchema();
            MessageColumnIO columnIO = ParquetTypeUtils.getColumnIO(fileSchema, fileSchema);

            ImmutableList.Builder<Column> columns = ImmutableList.builder();
            for (int i = 0; i < COLUMN_NAMES.size(); i++) {
                String columnName = COLUMN_NAMES.get(i);
                columns.add(new Column(columnName, ParquetTypeUtils
                        .constructField(COLUMN_TYPES.get(i), ParquetTypeUtils.lookupColumnByName(columnIO, columnName))
                        .orElseThrow()));
            }

            Map<List<String>, ColumnDescriptor> descriptorsByPath = ParquetTypeUtils.getDescriptors(fileSchema, fileSchema);
            TupleDomain<ColumnDescriptor> all = TupleDomain.all();
            TupleDomainParquetPredicate predicate =
                    PredicateUtils.buildPredicate(fileSchema, all, descriptorsByPath, DateTimeZone.UTC);
            List<RowGroupInfo> rowGroups = PredicateUtils.getFilteredRowGroups(
                    0, dataSource.getEstimatedSize(), dataSource, metadata,
                    ImmutableList.of(all), ImmutableList.of(predicate),
                    descriptorsByPath, DateTimeZone.UTC, 1000, options);

            int rows = 0;
            try (ParquetReader reader = new ParquetReader(
                    Optional.ofNullable(fileMetadata.getCreatedBy()),
                    columns.build(),
                    false,
                    rowGroups,
                    dataSource,
                    DateTimeZone.UTC,
                    newSimpleAggregatedMemoryContext(),
                    options,
                    e -> {
                        throwIfUnchecked(e);
                        return new RuntimeException(e);
                    },
                    Optional.of(predicate),
                    Optional.empty(),
                    Optional.empty())) {
                SourcePage page;
                while ((page = reader.nextPage()) != null) {
                    rows += page.getPositionCount();
                }
            }
            return rows;
        }
    }
}
