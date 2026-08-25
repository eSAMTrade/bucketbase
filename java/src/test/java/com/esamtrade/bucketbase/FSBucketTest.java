package com.esamtrade.bucketbase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Functional tests for {@link FSBucket}. The first block runs the shared {@link IBucketTester} suite that every backend must pass. The rest covers what is specific to a
 * filesystem backend: the temporary directory, the atomic rename, and concurrent writers over one root.
 */
class FSBucketTest {

    private static final int PARQUET_ROWS = 30_000;

    @TempDir
    Path tempRoot;

    private FSBucket storage;
    private IBucketTester tester;

    @BeforeEach
    void setUp() throws IOException {
        storage = new FSBucket(tempRoot);
        tester = new IBucketTester(storage);
    }

    @AfterEach
    void tearDown() throws IOException {
        tester.cleanup();
    }

    @Test
    void putAndGetObject() throws IOException {
        tester.testPutAndGetObject();
    }

    @Test
    void putObjectAndGetObjectStream() throws IOException {
        tester.testPutAndGetObjectStream();

        assertEquals(List.of(), temporaryDirectoryEntries(), "a completed write must leave no temporary file behind");
    }

    @Test
    void getObjectStreamIsSeekable() throws IOException {
        tester.testGetObjectStreamIsSeekable();
    }

    @Test
    void getListObjects() throws IOException {
        tester.testListObjects();
    }

    @Test
    void shallowListObjects() throws IOException {
        tester.testShallowListObjects();
    }

    @Test
    void listObjectsWithOver1000keys() throws IOException {
        tester.testListObjectsWithOver1000keys();
    }

    @Test
    void shallowListObjectsWithOver1000keys() throws IOException {
        tester.testShallowListObjectsWithOver1000keys();
    }

    @Test
    void testExists() throws IOException {
        tester.testExists();
    }

    @Test
    void testRemoveObjects() throws IOException {
        tester.testRemoveObjects();
    }

    @Test
    void testGetSize() throws IOException {
        tester.testGetSize();
    }

    @Test
    void fputAndFgetObject() throws IOException {
        tester.testFputAndFgetObject();
    }

    @Test
    void openWrite() throws IOException {
        tester.testOpenWrite();

        assertEquals(List.of(), temporaryDirectoryEntries(), "a committed write must leave no temporary file behind");
    }

    @Test
    void streamingFailureAtomicity() throws IOException {
        tester.testStreamingFailureAtomicity();

        assertEquals(List.of(), temporaryDirectoryEntries(), "a failed write must leave no temporary file behind");
    }

    @Test
    void putObjectStreamExceptionCleanup() throws IOException {
        tester.testPutObjectStreamExceptionCleanup();
    }

    @Test
    void openWritePartialWriteExceptionCleanup() throws IOException {
        tester.testOpenWritePartialWriteExceptionCleanup();
    }

    @Test
    void openWriteWithoutProperClose() throws IOException {
        tester.testOpenWriteWithoutProperClose();
    }

    /** Mirrors the Python {@code test_open_write_with_parquet}: a format that finalises on close must not publish the object when its writer closes the sink. */
    @Test
    void parquetFileStreamsThroughOpenWriteAndCommitsOnce() throws IOException {
        PurePosixPath name = PurePosixPath.from("parquet/fs-write-test.parquet");

        try (ObjectWriter writer = storage.openWrite(name)) {
            // writeParquet closes the ParquetWriter internally, which emits the footer and closes the sink.
            ParquetTestSupport.writeParquet(writer.stream(), PARQUET_ROWS);
            assertFalse(storage.exists(name), "the object must stay invisible after the Parquet footer is written but before commit()");
            writer.commit();
        }

        assertTrue(storage.exists(name));
        assertEquals(PARQUET_ROWS, ParquetTestSupport.countAllRows(storage, name));
        assertEquals(List.of(), temporaryDirectoryEntries());
    }

    /** Mirrors the Python {@code test_get_object_stream_with_parquet_tail}: the seekable stream must let a reader fetch the footer without downloading the object. */
    @Test
    void parquetFooterReadTouchesOnlyTheTail() throws IOException {
        PurePosixPath name = PurePosixPath.from("parquet/fs-read-test.parquet");
        byte[] parquet = ParquetTestSupport.parquetBytes(PARQUET_ROWS);
        storage.putObject(name, parquet);

        ParquetTestSupport.ReadResult footer = ParquetTestSupport.readFooterOnly(storage, name);

        assertEquals(PARQUET_ROWS / ParquetTestSupport.ROWS_PER_ROW_GROUP, footer.rowGroupsTotal());
        assertTrue(footer.bytesFetched() < parquet.length / 10,
                "footer read fetched " + footer.bytesFetched() + " of " + parquet.length + " bytes; expected under 10%");
    }

    /**
     * {@code getObjectStream} keeps a {@link java.nio.channels.FileChannel} open to serve positional reads, and hands it to the stream as its backing resource. Nothing else
     * releases it, so a stream that failed to close it would leak a descriptor per call. Counting the descriptors pointing at the object file is the only way to observe that
     * from a test; the process file-descriptor limit here is far too high for an open-many-streams loop to ever fail.
     */
    @Test
    @EnabledOnOs(OS.LINUX)
    void getObjectStreamReleasesTheFileHandleOnClose() throws IOException {
        PurePosixPath name = PurePosixPath.from("handles/leak-test.bin");
        storage.putObject(name, "payload".getBytes(StandardCharsets.UTF_8));
        Path file = tempRoot.resolve("handles").resolve("leak-test.bin").toRealPath();
        long before = countOpenHandlesTo(file);

        SeekableInputStream stream = storage.getObjectStream(name);
        assertEquals(before + 1, countOpenHandlesTo(file), "the stream must hold the object file open while it is readable");

        stream.close();
        assertEquals(before, countOpenHandlesTo(file), "close() must release the object file handle");
    }

    /** Descriptors in this process pointing at {@code file}. Entries that vanish mid-listing belong to another thread and are not counted. */
    private static long countOpenHandlesTo(Path file) throws IOException {
        try (Stream<Path> descriptors = Files.list(Path.of("/proc/self/fd"))) {
            return descriptors.filter(descriptor -> {
                try {
                    return Files.readSymbolicLink(descriptor).equals(file);
                } catch (IOException closedWhileListing) {
                    return false;
                }
            }).count();
        }
    }

    @Test
    void constructorCreatesTheRootDirectory() throws IOException {
        Path missingRoot = tempRoot.resolve("nested").resolve("bucket-root");

        FSBucket bucket = new FSBucket(missingRoot);

        assertTrue(Files.isDirectory(missingRoot));
        assertEquals(missingRoot.toAbsolutePath().normalize(), bucket.getRoot());
    }

    @Test
    void constructorRejectsARootThatIsAFile() throws IOException {
        Path file = Files.write(tempRoot.resolve("not-a-directory"), new byte[0]);

        assertThrows(IOException.class, () -> new FSBucket(file));
    }

    @Test
    void objectNameResolvingOutsideTheRootIsRejected() {
        // S3 stores ".." as an ordinary key component. On a filesystem the same key would read and write another directory, so FSBucket refuses it.
        PurePosixPath escaping = PurePosixPath.from("dir/../../escaped.txt");

        assertThrows(IllegalArgumentException.class, () -> storage.exists(escaping));
        assertThrows(IllegalArgumentException.class, () -> storage.putObject(escaping, new byte[]{1}));
        assertThrows(IllegalArgumentException.class, () -> storage.getObject(escaping));
        assertThrows(IllegalArgumentException.class, () -> storage.getSize(escaping));
        assertThrows(IllegalArgumentException.class, () -> storage.removeObjects(List.of(escaping)));
        assertFalse(Files.exists(tempRoot.getParent().resolve("escaped.txt")));
    }

    @Test
    void brokenSourceStreamStoresNothing() throws IOException {
        PurePosixPath name = PurePosixPath.from("broken_upload_test.txt");
        byte[] content = "This content must not be completely uploaded".getBytes(StandardCharsets.UTF_8);
        InputStream brokenStream = new ChunkedCallbackInputStream(content, 10,
                position -> {
                    assertFalse(storage.exists(name), "the object must stay invisible while its body is still being read");
                    if (position >= 10) {
                        throw new IOException("Simulated error during upload");
                    }
                }, null);

        assertThrows(IOException.class, () -> storage.putObjectStream(name, brokenStream));

        assertFalse(storage.exists(name));
        assertEquals(List.of(), storage.listObjects());
        ShallowListing listing = storage.shallowListObjects();
        assertEquals(List.of(), listing.objects());
        assertEquals(List.of(), listing.prefixes());
        assertEquals(List.of(), temporaryDirectoryEntries());
    }

    @Test
    void getSizeReportsNotFoundUntilTheWriteCompletes() throws IOException {
        PurePosixPath name = PurePosixPath.from("size_during_write_test.txt");
        byte[] content = "Testing byte-by-byte writing with size checks".getBytes(StandardCharsets.UTF_8);
        AtomicInteger checks = new AtomicInteger();

        // One byte per read, so the object is half-written for as long as possible.
        InputStream stream = new ChunkedCallbackInputStream(content, 1,
                position -> {
                    checks.incrementAndGet();
                    assertFalse(storage.exists(name));
                    assertThrows(FileNotFoundException.class, () -> storage.getSize(name), "the object must not exist at offset " + position);
                },
                position -> assertThrows(FileNotFoundException.class, () -> storage.getSize(name), "the object must not exist before the rename"));

        storage.putObjectStream(name, stream);

        assertTrue(storage.exists(name));
        assertEquals(content.length, storage.getSize(name));
        assertEquals(content.length, checks.get(), "the size must have been checked after every byte");
    }

    @Test
    void twoBucketsOverOneRootSeeTheSameObjects() throws IOException {
        FSBucket first = new FSBucket(tempRoot);
        FSBucket second = new FSBucket(tempRoot);
        PurePosixPath name = PurePosixPath.from("concurrent_test.txt");
        byte[] content = "Test content for concurrent access".getBytes(StandardCharsets.UTF_8);

        first.putObject(name, content);

        assertTrue(second.exists(name));
        assertEquals(new String(content, StandardCharsets.UTF_8), new String(second.getObject(name), StandardCharsets.UTF_8));

        first.removeObjects(List.of(name));

        assertFalse(second.exists(name));
        assertEquals(List.of(), second.listObjects());
    }

    @Test
    void listingsRejectEveryPrefixNamingTheTemporaryDirectory() {
        // The temporary directory name starts with "$", which no valid object key may contain, so no caller can list its way into it.
        List<String> prefixes = List.of(
                FSBucket.BUCKETBASE_TMP_DIR_NAME,
                FSBucket.BUCKETBASE_TMP_DIR_NAME + "/something",
                FSBucket.BUCKETBASE_TMP_DIR_NAME + "/",
                FSBucket.BUCKETBASE_TMP_DIR_NAME + ".something");

        for (String prefix : prefixes) {
            assertThrows(IllegalArgumentException.class, () -> storage.listObjects(PurePosixPrefix.from(prefix)), "listObjects accepted " + prefix);
            assertThrows(IllegalArgumentException.class, () -> storage.shallowListObjects(PurePosixPrefix.from(prefix)), "shallowListObjects accepted " + prefix);
        }
    }

    @Test
    void listingsHideTheTemporaryDirectory() throws IOException {
        PurePosixPath name = PurePosixPath.from("dir1/file.txt");
        storage.putObjectStream(name, new ByteArrayInputStream("Test content".getBytes(StandardCharsets.UTF_8)));
        Path temporaryDirectory = tempRoot.resolve(FSBucket.BUCKETBASE_TMP_DIR_NAME);
        assertTrue(Files.isDirectory(temporaryDirectory), "putObjectStream must have created the temporary directory");
        // A leftover from a process that died mid-write must not surface in any listing.
        Files.write(temporaryDirectory.resolve("dir1#file.txt@1-1.tmp"), "leftover".getBytes(StandardCharsets.UTF_8));

        assertEquals(List.of(name), storage.listObjects());
        assertEquals(List.of(name), storage.listObjects(PurePosixPrefix.from("d")));

        ShallowListing rootListing = storage.shallowListObjects();
        assertEquals(List.of(), rootListing.objects());
        assertEquals(List.of(PurePosixPrefix.from("dir1/")), rootListing.prefixes());

        ShallowListing narrowedListing = storage.shallowListObjects(PurePosixPrefix.from("d"));
        assertEquals(List.of(), narrowedListing.objects());
        assertEquals(List.of(PurePosixPrefix.from("dir1/")), narrowedListing.prefixes());
    }

    @Test
    void concurrentWritersOfTheSameKeyProduceOneObject() throws Exception {
        runConcurrentWriters("test/dir/for/concurrent_test.txt", 9, 1);
    }

    @Test
    void concurrentWritersOfDistinctKeysProduceOneObjectEach() throws Exception {
        runConcurrentWriters(null, 9, 9);
    }

    /**
     * Starts {@code writerCount} threads that each stream an object into the same root and hold their bodies open at once, then checks that no object is visible while the
     * writes are in flight and that exactly {@code expectedObjects} appear once they finish.
     *
     * @param sharedName the key every writer writes, or {@code null} to give each writer its own key
     */
    private void runConcurrentWriters(String sharedName, int writerCount, int expectedObjects) throws Exception {
        CyclicBarrier bodiesRead = new CyclicBarrier(writerCount + 1);
        CyclicBarrier renamesReleased = new CyclicBarrier(writerCount + 1);
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        List<Thread> writers = new ArrayList<>();

        for (int i = 0; i < writerCount; i++) {
            String key = sharedName != null ? sharedName : "test/dir/for/concurrent_test_" + i + ".txt";
            Thread writer = new Thread(() -> {
                try {
                    // A separate instance per thread, as separate processes over a shared directory would have.
                    FSBucket bucket = new FSBucket(tempRoot);
                    InputStream body = new ChunkedCallbackInputStream("Testing concurrent streams".getBytes(StandardCharsets.UTF_8), 1, null,
                            position -> {
                                // The body is fully read but the temporary file is not renamed yet: hold every writer here at once.
                                awaitBarrier(bodiesRead);
                                awaitBarrier(renamesReleased);
                            });
                    bucket.putObjectStream(PurePosixPath.from(key), body);
                } catch (Throwable failure) {
                    writerFailure.compareAndSet(null, failure);
                }
            }, "fs-writer-" + i);
            writers.add(writer);
            writer.start();
        }

        bodiesRead.await();
        assertEquals(List.of(), storage.listObjects(), "no object may be visible while the writes are still in flight");
        assertEquals(writerCount, temporaryDirectoryEntries().size(), "every in-flight write must hold its own temporary file");

        renamesReleased.await();
        for (Thread writer : writers) {
            writer.join();
        }

        if (writerFailure.get() != null) {
            throw new AssertionError("a writer thread failed", writerFailure.get());
        }
        assertEquals(List.of(), temporaryDirectoryEntries(), "every completed write must have removed its temporary file");
        assertEquals(expectedObjects, storage.listObjects().size());
    }

    private static void awaitBarrier(CyclicBarrier barrier) throws IOException {
        try {
            barrier.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for the other writers", interrupted);
        } catch (BrokenBarrierException broken) {
            throw new IOException("Another writer failed while waiting at the barrier", broken);
        }
    }

    private List<String> temporaryDirectoryEntries() throws IOException {
        Path temporaryDirectory = tempRoot.resolve(FSBucket.BUCKETBASE_TMP_DIR_NAME);
        if (!Files.isDirectory(temporaryDirectory)) {
            return List.of();
        }
        try (Stream<Path> entries = Files.list(temporaryDirectory)) {
            return entries.map(path -> path.getFileName().toString()).sorted().toList();
        }
    }

    /** Serves content in fixed-size chunks, calling back after each chunk and once at end of stream, so a test can observe the bucket mid-write. */
    private static final class ChunkedCallbackInputStream extends InputStream {

        @FunctionalInterface
        interface PositionCallback {
            void at(int position) throws IOException;
        }

        private final byte[] content;
        private final int chunkSize;
        private final PositionCallback afterChunk;
        private final PositionCallback atEndOfStream;
        private int position;
        private boolean endReported;

        ChunkedCallbackInputStream(byte[] content, int chunkSize, PositionCallback afterChunk, PositionCallback atEndOfStream) {
            this.content = content;
            this.chunkSize = chunkSize;
            this.afterChunk = afterChunk;
            this.atEndOfStream = atEndOfStream;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            return read(one, 0, 1) == -1 ? -1 : Byte.toUnsignedInt(one[0]);
        }

        @Override
        public int read(byte[] destination, int offset, int length) throws IOException {
            if (position >= content.length) {
                if (!endReported) {
                    endReported = true;
                    if (atEndOfStream != null) {
                        atEndOfStream.at(position);
                    }
                }
                return -1;
            }
            int read = Math.min(Math.min(length, chunkSize), content.length - position);
            System.arraycopy(content, position, destination, offset, read);
            position += read;
            if (afterChunk != null) {
                afterChunk.at(position);
            }
            return read;
        }
    }
}
