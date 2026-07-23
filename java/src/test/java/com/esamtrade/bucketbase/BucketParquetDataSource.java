package com.esamtrade.bucketbase;

import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;

import java.io.IOException;

/**
 * Adapts a bucketbase object to Trino's {@link io.trino.parquet.ParquetDataSource} so that a
 * real Parquet reader can drive the seekable, range-backed stream returned by
 * {@link IBucket#getObjectStream(PurePosixPath)}.
 *
 * <p>This lives in test scope on purpose: it proves the stream is sufficient for partial reads,
 * column projection and predicate pushdown without making bucketbase itself depend on Trino or
 * on any Parquet implementation. A shipped adapter belongs in a separate optional module.</p>
 *
 * <p>Every byte the Parquet reader pulls funnels through {@link #readInternal}, so the counters
 * here are an exact account of what the reader actually fetched.</p>
 */
public final class BucketParquetDataSource extends AbstractParquetDataSource {

    private final SeekableInputStream stream;

    private long bytesFetched;
    private int rangeRequests;

    private BucketParquetDataSource(PurePosixPath name, SeekableInputStream stream, ParquetReaderOptions options) {
        // The stream already knows the object size, so this costs no extra request.
        super(new ParquetDataSourceId(name.toString()), stream.size(), options);
        this.stream = stream;
    }

    public static BucketParquetDataSource open(IBucket bucket, PurePosixPath name, ParquetReaderOptions options) throws IOException {
        return new BucketParquetDataSource(name, bucket.getObjectStream(name), options);
    }

    /** Total bytes the Parquet reader pulled out of the bucket. */
    public long bytesFetched() {
        return bytesFetched;
    }

    /** Number of distinct ranged reads issued. */
    public int rangeRequests() {
        return rangeRequests;
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength) throws IOException {
        bytesFetched += bufferLength;
        rangeRequests++;
        stream.seek(position);
        int read = stream.readNBytes(buffer, bufferOffset, bufferLength);
        if (read != bufferLength) {
            throw new IOException("Expected " + bufferLength + " bytes at offset " + position + " but got " + read);
        }
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
