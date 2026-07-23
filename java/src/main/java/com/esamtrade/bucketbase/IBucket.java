package com.esamtrade.bucketbase;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public interface IBucket extends Closeable {
    int MINIO_PATH_TEMP_SUFFIX_LEN = 43;
    int WINDOWS_MAX_PATH = 260;
    String DEFAULT_ENCODING = "utf-8";

    /** Characters allowed in an object name besides the {@code /} separator; matches the Python library. */
    String S3_NAME_CHARS_NO_SEP = "\\w!\\-\\.')(";

    /**
     * Releases any resources this bucket owns (for example S3 clients it created itself).
     * The default is a no-op; backends that hold no resources need not override it.
     */
    @Override
    default void close() throws IOException {
    }

    void putObject(PurePosixPath name, byte[] content) throws IOException;

    void putObjectStream(PurePosixPath name, InputStream stream) throws IOException;

    byte[] getObject(PurePosixPath name) throws IOException;

    /**
     * Returns a readable, closeable stream positioned at the start of the object that supports
     * querying its position and seeking to any non-negative offset.
     */
    SeekableInputStream getObjectStream(PurePosixPath name) throws IOException;

    /**
     * Returns the size of the object in bytes.
     *
     * @throws java.io.FileNotFoundException if the object does not exist
     */
    long getSize(PurePosixPath name) throws IOException;

    /**
     * Opens a streaming sink for {@code name}. Bytes are uploaded as they are written, and the
     * object becomes visible only once {@link ObjectWriter#commit()} succeeds - abandoning the
     * writer leaves no object behind.
     *
     * <p>This is the counterpart of the Python library's {@code IBucket.open_write()} and is the
     * intended way to write large or incrementally produced payloads such as Parquet files.</p>
     *
     * @see ObjectWriter
     */
    default ObjectWriter openWrite(PurePosixPath name) throws IOException {
        return openWrite(name, ObjectWriter.DEFAULT_TIMEOUT_MILLIS);
    }

    /**
     * As {@link #openWrite(PurePosixPath)}, but bounds how long commit and abort wait for the
     * background uploader to settle.
     */
    default ObjectWriter openWrite(PurePosixPath name, long timeoutMillis) throws IOException {
        return new ObjectWriter(this, name, timeoutMillis);
    }

    List<PurePosixPath> listObjects(PurePosixPath prefix) throws IOException;

    ShallowListing shallowListObjects(PurePosixPath prefix) throws IOException;

    boolean exists(PurePosixPath name) throws IOException;

    List<DeleteError> removeObjects(List<PurePosixPath> names) throws IOException;

    default void fputObject(PurePosixPath name, Path filePath) throws IOException {
        byte[] content = Files.readAllBytes(filePath);
        putObject(name, content);
    }

    default void fgetObject(PurePosixPath name, Path filePath) throws IOException {
        String randomSuffix = UUID.randomUUID().toString().substring(0, 8);
        Path parent = filePath.toAbsolutePath().getParent();
        Path tmpFilePath = parent.resolve("_" + filePath.getFileName() + "." + randomSuffix + ".part");

        try {
            byte[] response = getObject(name);
            Files.write(tmpFilePath, response);
            Files.move(tmpFilePath, filePath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException exc) {
            if (System.getProperty("os.name").toLowerCase().contains("win")) {
                if (tmpFilePath.toString().length() >= WINDOWS_MAX_PATH - MINIO_PATH_TEMP_SUFFIX_LEN) {
                    throw new IllegalArgumentException(
                            "Reduce the Minio cache path length, Windows has limitation on the path length. " +
                                    "More details here: https://docs.python.org/3/using/windows.html#removing-the-max-path-limitation",
                            exc
                    );
                }
            }
            throw exc;
        } finally {
            Files.deleteIfExists(tmpFilePath);
        }
    }

    default void removePrefix(PurePosixPath prefix) throws IOException {
        List<PurePosixPath> objects = listObjects(prefix);
        removeObjects(objects);
    }

    /**
     * Copies an object from another bucket into this one, streaming it through without holding
     * the whole object in memory.
     */
    default void copyObjectFrom(IBucket srcBucket, PurePosixPath srcName, PurePosixPath dstName) throws IOException {
        try (SeekableInputStream stream = srcBucket.getObjectStream(srcName)) {
            putObjectStream(dstName, stream);
        }
    }

    /**
     * Copies every object under {@code srcPrefix} from this bucket to {@code dstPrefix} in
     * {@code dstBucket}, using up to {@code threads} worker threads.
     */
    default void copyPrefix(IBucket dstBucket, PurePosixPath srcPrefix, PurePosixPath dstPrefix, int threads) throws IOException {
        if (threads <= 0) {
            throw new IllegalArgumentException("threads must be greater than 0");
        }
        List<PurePosixPath> srcObjects = listObjects(srcPrefix);
        if (srcObjects.isEmpty()) {
            return;
        }
        int srcPrefixLen = srcPrefix.toString().length();
        String dstPrefixStr = dstPrefix.toString();

        ExecutorService executor = Executors.newFixedThreadPool(Math.min(threads, srcObjects.size()));
        try {
            List<Future<?>> futures = new ArrayList<>(srcObjects.size());
            for (PurePosixPath srcObj : srcObjects) {
                futures.add(executor.submit(() -> {
                    String name = dstPrefixStr + srcObj.toString().substring(srcPrefixLen);
                    if (name.startsWith("/")) {
                        name = name.substring(1);
                    }
                    dstBucket.putObject(PurePosixPath.from(name), getObject(srcObj));
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw cause instanceof IOException io ? io : new IOException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while copying prefix " + srcPrefix, e);
        } finally {
            executor.shutdownNow();
        }
    }

    default void movePrefix(IBucket dstBucket, PurePosixPath srcPrefix, PurePosixPath dstPrefix, int threads) throws IOException {
        copyPrefix(dstBucket, srcPrefix, dstPrefix, threads);
        removePrefix(srcPrefix);
    }
}
