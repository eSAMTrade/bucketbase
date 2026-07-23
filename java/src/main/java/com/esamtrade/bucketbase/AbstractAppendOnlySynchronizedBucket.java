package com.esamtrade.bucketbase;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;

/**
 * A write-once wrapper around another bucket: an object may be created but never overwritten or
 * deleted, and concurrent writes to the same key are serialized by a lock provided by the
 * concrete subclass. Reads are unsynchronized because a completed write is atomic.
 *
 * <p>Mirrors the Python library's {@code AbstractAppendOnlySynchronizedBucket}.</p>
 */
public abstract class AbstractAppendOnlySynchronizedBucket implements IBucket {
    private final IBucket baseBucket;

    protected AbstractAppendOnlySynchronizedBucket(IBucket baseBucket) {
        this.baseBucket = Objects.requireNonNull(baseBucket, "baseBucket");
    }

    /** The wrapped bucket that all operations delegate to. */
    protected IBucket baseBucket() {
        return baseBucket;
    }

    @Override
    public void putObject(PurePosixPath name, byte[] content) throws IOException {
        lockObject(name);
        try {
            baseBucket.putObject(name, content);
        } finally {
            unlockObject(name);
        }
    }

    @Override
    public void putObjectStream(PurePosixPath name, InputStream stream) throws IOException {
        lockObject(name);
        try {
            baseBucket.putObjectStream(name, stream);
        } finally {
            unlockObject(name);
        }
    }

    @Override
    public byte[] getObject(PurePosixPath name) throws IOException {
        return baseBucket.getObject(name);
    }

    @Override
    public SeekableInputStream getObjectStream(PurePosixPath name) throws IOException {
        return baseBucket.getObjectStream(name);
    }

    @Override
    public long getSize(PurePosixPath name) throws IOException {
        return baseBucket.getSize(name);
    }

    @Override
    public List<PurePosixPath> listObjects(PurePosixPath prefix) throws IOException {
        return baseBucket.listObjects(prefix);
    }

    @Override
    public ShallowListing shallowListObjects(PurePosixPath prefix) throws IOException {
        return baseBucket.shallowListObjects(prefix);
    }

    @Override
    public boolean exists(PurePosixPath name) throws IOException {
        return baseBucket.exists(name);
    }

    @Override
    public List<DeleteError> removeObjects(List<PurePosixPath> names) {
        throw new UnsupportedOperationException("removeObjects is not supported for an append-only bucket");
    }

    @Override
    public void removePrefix(PurePosixPath prefix) {
        throw new UnsupportedOperationException("removePrefix is not supported for an append-only bucket");
    }

    @Override
    public void movePrefix(IBucket dstBucket, PurePosixPath srcPrefix, PurePosixPath dstPrefix, int threads) {
        // Fail fast: the copy would otherwise succeed and only the delete step would fail, leaving
        // the data duplicated.
        throw new UnsupportedOperationException("movePrefix is not supported for an append-only bucket");
    }

    @Override
    public void close() throws IOException {
        baseBucket.close();
    }

    protected abstract void lockObject(PurePosixPath name);

    protected abstract void unlockObject(PurePosixPath name);
}
