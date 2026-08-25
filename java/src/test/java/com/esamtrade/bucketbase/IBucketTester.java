package com.esamtrade.bucketbase;

import org.junit.jupiter.api.function.Executable;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IBucketTester {

    private static final List<String> INVALID_PREFIXES = List.of("/", "/dir", "star*1", "dir1/a\\file.txt", "at@gmail", "sharp#1", "dollar$1", "comma,");
    /** Bounds every ObjectWriter test, so a stuck uploader fails the test instead of hanging the build for the five-minute default. */
    private static final long WRITER_TIMEOUT_MILLIS = 30_000L;
    private final IBucket storage;
    private final String uniqueSuffix;
    private final String PATH_WITH_2025_KEYS = "test-dir-with-2025-keys/";
    private static final int KEYS_IN_LARGE_LISTING_FIXTURE = 2025;
    /**
     * Serializes creation of the large-listing fixture. Unlike every other key these tests touch, its path is fixed rather than per-tester, so two testers sharing a bucket
     * would otherwise race: one lists the prefix while the other is half-way through creating it, and sees a count between zero and the full set.
     */
    private static final Object LARGE_LISTING_FIXTURE_LOCK = new Object();

    public IBucketTester(IBucket storage) {
        this.storage = storage;
        // Every tester owns a private key namespace. A clock-derived suffix collides when two tests start in the same millisecond, which is the normal case once the suite
        // runs in parallel against one shared bucket. Matches the Python framework's uuid4().hex.
        this.uniqueSuffix = UUID.randomUUID().toString().replace("-", "");
    }

    public void cleanup() throws IOException {
        storage.removePrefix(PurePosixPrefix.from("dir" + uniqueSuffix));
    }

    public void testPutAndGetObject() throws IOException {
        String uniqueDir = "dir" + uniqueSuffix;

        // Binary content
        PurePosixPath path = PurePosixPath.from(uniqueDir, "file1.bin");

        byte[] bContent = "Test content".getBytes();
        storage.putObject(path, bContent);
        byte[] retrievedContent = storage.getObject(path);
        assertArrayEquals(retrievedContent, bContent);

        // String content
        path = PurePosixPath.from(uniqueDir, "file1.txt");
        String sContent = "Test content";
        storage.putObject(path, sContent.getBytes());
        retrievedContent = storage.getObject(path);
        assertArrayEquals(retrievedContent, sContent.getBytes("utf-8"));

        // ByteArray content
        path = PurePosixPath.from(uniqueDir, "file1.ba");
        byte[] baContent = "Test content".getBytes();
        storage.putObject(path, baContent);
        retrievedContent = storage.getObject(path);
        assertArrayEquals(retrievedContent, baContent);

        // String path
        String stringPath = uniqueDir + "/file1.txt";
        storage.putObject(PurePosixPath.from(stringPath), sContent.getBytes());
        retrievedContent = storage.getObject(PurePosixPath.from(stringPath));
        assertArrayEquals(retrievedContent, sContent.getBytes("utf-8"));

        // Non-existent path
        PurePosixPath nonExistentPath = PurePosixPath.from(uniqueDir, "inexistent.txt");
        assertThrows(FileNotFoundException.class, () -> storage.getObject(nonExistentPath), "Expected exception not thrown for non-existent path");
    }

    public void testPutAndGetObjectStream() throws IOException {
        String uniqueDir = "dir" + uniqueSuffix;

        // Binary content
        PurePosixPath path = PurePosixPath.from(uniqueDir, "file1.bin");
        byte[] bContent = "Test\ncontent".getBytes();
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOut = new GZIPOutputStream(byteStream)) {
            gzipOut.write(bContent);
        }
        ByteArrayInputStream gzippedStream = new ByteArrayInputStream(byteStream.toByteArray());

        storage.putObjectStream(path, gzippedStream);
        try (SeekableInputStream file = storage.getObjectStream(path)) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(file)))) {
                String[] result = new String[3];
                for (int i = 0; i < 3; i++) {
                    result[i] = reader.readLine();
                }
                assertArrayEquals(new String[]{"Test", "content", null}, result);
            }
        }

        // String path
        path = PurePosixPath.from(uniqueDir, "file1.bin");
        try (SeekableInputStream file = storage.getObjectStream(path)) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(file)))) {
                char[] cbuf = new char[100];
                int readCount = reader.read(cbuf, 0, 100);
                String result = new String(cbuf, 0, readCount);
                assertEquals("Test\ncontent", result);
            }
        }

        // Non-existent path
        PurePosixPath nonExistentPath = PurePosixPath.from(uniqueDir, "inexistent.txt");
        assertThrows(FileNotFoundException.class, () -> storage.getObjectStream(nonExistentPath));
    }

    public void testGetObjectStreamIsSeekable() throws IOException {
        String uniqueDir = "dir" + uniqueSuffix;
        PurePosixPath path = PurePosixPath.from(uniqueDir, "seekable.bin");
        byte[] content = "0123456789abcdefghijklmnopqrstuvwxyz".getBytes();
        storage.putObject(path, content);

        try (SeekableInputStream stream = storage.getObjectStream(path)) {
            assertEquals(content.length, stream.size());
            assertArrayEquals("0123".getBytes(), stream.readNBytes(4));
            stream.seek(10);
            assertArrayEquals("abcd".getBytes(), stream.readNBytes(4));
            stream.seek(stream.position() - 2);
            assertArrayEquals("cd".getBytes(), stream.readNBytes(2));
            stream.seek(stream.size() - 4);
            assertArrayEquals("wxyz".getBytes(), stream.readNBytes(4));
            stream.seek(stream.size() + 10);
            assertEquals(-1, stream.read());
            assertThrows(IOException.class, () -> stream.seek(-1));
        }
    }

    public void testListObjects() throws IOException {
        String uniqueDir = "dir" + uniqueSuffix;
        storage.putObject(PurePosixPath.from(uniqueDir, "file1.txt"), "Content 1".getBytes());
        storage.putObject(PurePosixPath.from(uniqueDir, "dir2/file2.txt"), "Content 2".getBytes());
        storage.putObject(PurePosixPath.from(uniqueDir + "file1.txt"), "Content 3".getBytes());

        List<PurePosixPath> objects = storage.listObjects(PurePosixPrefix.from(uniqueDir)).stream().sorted().toList();
        List<PurePosixPath> expectedObjects = List.of(
                PurePosixPath.from(uniqueDir, "dir2/file2.txt"),
                PurePosixPath.from(uniqueDir, "file1.txt"),
                PurePosixPath.from(uniqueDir + "file1.txt")
                                                     );
        assertEquals(expectedObjects, objects);

        objects = storage.listObjects(PurePosixPrefix.from(uniqueDir + "/")).stream().sorted().toList();
        expectedObjects = List.of(
                PurePosixPath.from(uniqueDir, "dir2/file2.txt"),
                PurePosixPath.from(uniqueDir, "file1.txt")
                                 );
        assertEquals(expectedObjects, objects);

        // Invalid Prefix cases
        for (String prefix : INVALID_PREFIXES) {
            assertThrows(IllegalArgumentException.class, () -> storage.listObjects(PurePosixPrefix.from(prefix)), "Invalid prefix: " + prefix);
        }
    }

    public void testListObjectsWithOver1000keys() throws IOException {
        // Check if PATH_WITH_2025_KEYS exists
        var pathWith2025Keys = ensureDirWith2025Keys();

        List<PurePosixPath> objects = storage.listObjects(pathWith2025Keys);
        assertEquals(KEYS_IN_LARGE_LISTING_FIXTURE, objects.size());
    }

    /**
     * Returns the prefix of the large-listing fixture, creating it if the bucket does not already hold the whole set. Completing a partial set also repairs a fixture left
     * behind by a run that died half-way through.
     */
    private PurePosixPrefix ensureDirWith2025Keys() throws IOException {
        var pathWith2025Keys = new PurePosixPrefix(PATH_WITH_2025_KEYS);
        synchronized (LARGE_LISTING_FIXTURE_LOCK) {
            if (storage.listObjects(pathWith2025Keys).size() >= KEYS_IN_LARGE_LISTING_FIXTURE) {
                return pathWith2025Keys;
            }
            try (ForkJoinPool customThreadPool = new ForkJoinPool(50)) {
                try {
                    customThreadPool.submit(() ->
                                    IntStream.range(0, KEYS_IN_LARGE_LISTING_FIXTURE).parallel().forEach(i -> {
                                        try {
                                            var path = pathWith2025Keys.join(
                                                    new PurePosixPath("file" + i + ".txt"));
                                            storage.putObject(path, ("Content " + i).getBytes());
                                        } catch (IOException e) {
                                            throw new RuntimeException(e);
                                        }
                                    })
                                           ).get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    customThreadPool.shutdown();
                }
            }
        }
        return pathWith2025Keys;
    }

    public void testShallowListObjectsWithOver1000keys() throws IOException {
        // Check if PATH_WITH_2025_KEYS exists
        var pathWith2025Keys = ensureDirWith2025Keys();

        ShallowListing objects = storage.shallowListObjects(pathWith2025Keys);
        assertEquals(KEYS_IN_LARGE_LISTING_FIXTURE, objects.objects().size());
        assertEquals(0, objects.prefixes().size());
    }

    public void testShallowListObjects() throws IOException {
        String uniqueDir = "dir" + uniqueSuffix;
        storage.putObject(new PurePosixPath(uniqueDir + "/file1.txt"), "Content 1".getBytes());
        storage.putObject(new PurePosixPath(uniqueDir + "/dir2/file2.txt"), "Content 2".getBytes());
        storage.putObject(new PurePosixPath(uniqueDir + "file1.txt"), "Content 3".getBytes());

        assertThrows(IllegalArgumentException.class, () -> storage.shallowListObjects(new PurePosixPrefix("/")));
        assertThrows(IllegalArgumentException.class, () -> storage.shallowListObjects(new PurePosixPrefix("/d")));

        ShallowListing objects = storage.shallowListObjects(new PurePosixPrefix(uniqueDir + "/"));
        List<PurePosixPath> expectedObjects = List.of(PurePosixPath.from(uniqueDir + "/file1.txt"));
        List<PurePosixPrefix> expectedPrefixes = List.of(PurePosixPrefix.from(uniqueDir + "/dir2/"));
        assertIterableEquals(expectedObjects, objects.objects());
        assertIterableEquals(expectedPrefixes, objects.prefixes());

        ShallowListing shallowListing = storage.shallowListObjects(new PurePosixPrefix(uniqueDir));
        expectedObjects = List.of(new PurePosixPath(uniqueDir + "file1.txt"));
        expectedPrefixes = List.of(PurePosixPrefix.from(uniqueDir + "/"));
        assertIterableEquals(expectedObjects, shallowListing.objects());
        assertIterableEquals(expectedPrefixes, shallowListing.prefixes());

        // Invalid Prefix cases
        for (String prefix : INVALID_PREFIXES) {
            assertThrows(IllegalArgumentException.class, () -> storage.shallowListObjects(new PurePosixPrefix(prefix)));
        }
    }

    public void testExists() throws IOException {
        String uniqueDir = "dir" + uniqueSuffix;
        PurePosixPath path = new PurePosixPath(uniqueDir + "/file.txt");
        storage.putObject(path, "Content".getBytes());

        assertTrue(storage.exists(path));
        assertFalse(storage.exists(new PurePosixPath(uniqueDir)));
        assertThrows(IllegalArgumentException.class, () -> storage.exists(new PurePosixPath(uniqueDir + "*")));
    }

    public void testRemoveObjects() throws IOException {
        // Setup the test
        String uniqueDir = "dir" + uniqueSuffix;
        PurePosixPath path1 = new PurePosixPath(uniqueDir + "/file1.txt");
        PurePosixPath path2 = new PurePosixPath(uniqueDir + "/file2.txt");
        storage.putObject(path1, "Content 1".getBytes());
        storage.putObject(path2, "Content 2".getBytes());

        // Perform removal action
        List<DeleteError> result = storage.removeObjects(List.of(path1, path2, new PurePosixPath(uniqueDir + "/inexistent.file")));

        // Check that the files do not exist
        assertTrue(result instanceof List);
        assertEquals(List.of(), result);
        assertFalse(storage.exists(path1));
        assertFalse(storage.exists(path2));
        assertThrows(FileNotFoundException.class, () -> storage.getObject(new PurePosixPath(uniqueDir + "/file1.txt")));
        assertThrows(IllegalArgumentException.class, () -> storage.removeObjects(List.of(new PurePosixPath(uniqueDir + "*"))));

        // Check that the leftover empty directories are also removed, but the bucket may contain leftovers from the other test runs
        ShallowListing shallowListing = storage.shallowListObjects(new PurePosixPrefix(""));
        List<PurePosixPrefix> prefixes = shallowListing.prefixes();
        assertFalse(prefixes.contains(PurePosixPrefix.from(uniqueDir + "/")));
    }

    public void testGetSize() throws IOException {
        String uniqueDir = "dir" + uniqueSuffix;
        PurePosixPath path = PurePosixPath.from(uniqueDir, "file1.txt");

        byte[] content = "Content 1".getBytes(StandardCharsets.UTF_8);
        storage.putObject(path, content);
        assertEquals(content.length, storage.getSize(path));

        assertThrows(FileNotFoundException.class, () -> storage.getSize(PurePosixPath.from(uniqueDir, "NOT.FOUND")));

        // Overwriting the object must report the new size, not the size it was created with.
        byte[] modified = "Content 1 -- modified".getBytes(StandardCharsets.UTF_8);
        storage.putObject(path, modified);
        assertEquals(modified.length, storage.getSize(path));
    }

    public void testFputAndFgetObject() throws IOException {
        String uniqueDir = "dir" + uniqueSuffix;
        PurePosixPath name = PurePosixPath.from(uniqueDir, "roundtrip.bin");
        byte[] content = "fput and fget round trip".getBytes(StandardCharsets.UTF_8);

        Path workDir = Files.createTempDirectory("bucketbase-fput");
        try {
            Path source = Files.write(workDir.resolve("source.bin"), content);
            storage.fputObject(name, source);
            assertArrayEquals(content, storage.getObject(name));

            Path destination = workDir.resolve("destination.bin");
            storage.fgetObject(name, destination);
            assertArrayEquals(content, Files.readAllBytes(destination));

            // The second download must overwrite the file the first one wrote.
            storage.fgetObject(name, destination);
            assertArrayEquals(content, Files.readAllBytes(destination));

            assertThrows(FileNotFoundException.class,
                    () -> storage.fgetObject(PurePosixPath.from(uniqueDir, "inexistent.bin"), workDir.resolve("never-written.bin")));
            assertFalse(Files.exists(workDir.resolve("never-written.bin")));
        } finally {
            deleteRecursively(workDir);
        }
    }

    /**
     * Writes two objects through {@link ObjectWriter} and reads them back: one a gzip stream produced by a nested writer that closes the sink, one a plain multi-chunk body.
     */
    public void testOpenWrite() throws IOException {
        String uniqueDir = "dir" + uniqueSuffix;
        List<String> rows = List.of("id,name,value", "1,one,1.1", "2,two,2.2", "3,three,3.3");

        PurePosixPath gzipPath = PurePosixPath.from(uniqueDir, "multipart_large.csv.gz");
        try (ObjectWriter writer = storage.openWrite(gzipPath, WRITER_TIMEOUT_MILLIS)) {
            // GZIPOutputStream.close() writes the trailer and then closes the sink. The sink must ignore that close, otherwise the object would commit itself here.
            try (Writer csv = new OutputStreamWriter(new GZIPOutputStream(writer.stream()), StandardCharsets.UTF_8)) {
                for (String row : rows) {
                    csv.write(row + "\n");
                }
            }
            writer.commit();
        }

        try (SeekableInputStream stream = storage.getObjectStream(gzipPath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(stream), StandardCharsets.UTF_8))) {
            assertEquals(rows, reader.lines().toList());
        }
        assertTrue(storage.exists(gzipPath));
        assertEquals(storage.getObject(gzipPath).length, storage.getSize(gzipPath));

        // A body large enough to cross the write buffer of every backend, written in several calls.
        PurePosixPath chunkedPath = PurePosixPath.from(uniqueDir, "multipart_file.bin");
        byte[] chunk = new byte[64 * 1024];
        IntStream.range(0, chunk.length).forEach(i -> chunk[i] = (byte) i);
        try (ObjectWriter writer = storage.openWrite(chunkedPath, WRITER_TIMEOUT_MILLIS)) {
            for (int i = 0; i < 5; i++) {
                writer.stream().write(chunk);
            }
            writer.commit();
        }
        assertEquals(5L * chunk.length, storage.getSize(chunkedPath));

        byte[] stored = storage.getObject(chunkedPath);
        for (int i = 0; i < 5; i++) {
            assertArrayEquals(chunk, Arrays.copyOfRange(stored, i * chunk.length, (i + 1) * chunk.length), "chunk " + i + " was stored incorrectly");
        }
    }

    /**
     * A write that fails part way must leave no object behind. Every case here asserts the same thing for a different failure: the object the caller asked for does not exist.
     */
    public void testStreamingFailureAtomicity() throws IOException {
        String uniqueDir = "dir" + uniqueSuffix;

        // The source stream throws while the backend reads it.
        PurePosixPath failedRead = PurePosixPath.from(uniqueDir, "failed_stream_read.bin");
        assertFalse(storage.exists(failedRead));
        assertInjectedFailureReachedTheCaller(
                () -> storage.putObjectStream(failedRead, new FailingInputStream("This content must not be stored".getBytes(StandardCharsets.UTF_8), 10)));
        assertFalse(storage.exists(failedRead), "no object may exist after putObjectStream failed");

        // The caller throws inside the try-with-resources block of an open writer.
        PurePosixPath failedWrite = PurePosixPath.from(uniqueDir, "failed_open_write.bin");
        assertFalse(storage.exists(failedWrite));
        assertThrows(MockException.class, () -> {
            try (ObjectWriter writer = storage.openWrite(failedWrite, WRITER_TIMEOUT_MILLIS)) {
                writer.stream().write("Some initial data".getBytes(StandardCharsets.UTF_8));
                throw new MockException("Simulated write failure");
            }
        });
        assertFalse(storage.exists(failedWrite), "no object may exist after an aborted openWrite");

        // The local file handed to fputObject does not exist.
        PurePosixPath failedFput = PurePosixPath.from(uniqueDir, "failed_fput.bin");
        assertFalse(storage.exists(failedFput));
        assertThrows(IOException.class, () -> storage.fputObject(failedFput, Path.of("/non/existent/file.txt")));
        assertFalse(storage.exists(failedFput), "no object may exist after fputObject failed");

        // The source stream throws only after several buffer-sized reads.
        PurePosixPath failedPartial = PurePosixPath.from(uniqueDir, "failed_partial_stream.bin");
        byte[] largeContent = new byte[3 * 1024];
        assertFalse(storage.exists(failedPartial));
        assertInjectedFailureReachedTheCaller(() -> storage.putObjectStream(failedPartial, new FailingInputStream(largeContent, 1_500)));
        assertFalse(storage.exists(failedPartial), "no object may exist after a source stream failed mid-read");

        // The bucket still accepts writes after all those failures.
        PurePosixPath afterFailures = PurePosixPath.from(uniqueDir, "successful_after_failures.bin");
        byte[] content = "This must succeed after the previous failures".getBytes(StandardCharsets.UTF_8);
        storage.putObjectStream(afterFailures, new ByteArrayInputStream(content));
        assertTrue(storage.exists(afterFailures));
        assertArrayEquals(content, storage.getObject(afterFailures));

        assertThrows(FileNotFoundException.class, () -> storage.getObjectStream(PurePosixPath.from(uniqueDir, "inexistent.txt")));
    }

    /** A failed {@code putObjectStream} must leave nothing listable, not merely nothing readable. */
    public void testPutObjectStreamExceptionCleanup() throws IOException {
        String uniqueDir = "dir" + uniqueSuffix;
        PurePosixPath path = PurePosixPath.from(uniqueDir, "stream_exception_test.txt");
        byte[] content = new byte[1_000];

        assertFalse(storage.exists(path));
        assertInjectedFailureReachedTheCaller(() -> storage.putObjectStream(path, new FailingInputStream(content, 500)));

        assertFalse(storage.exists(path), "no object may exist after putObjectStream failed");
        assertEquals(List.of(), storage.listObjects(PurePosixPrefix.from(uniqueDir)), "a failed write must not appear in a listing");
    }

    /** An exception raised after several successful writes must abort the object, not commit what was written so far. */
    public void testOpenWritePartialWriteExceptionCleanup() throws IOException {
        String uniqueDir = "dir" + uniqueSuffix;
        PurePosixPath path = PurePosixPath.from(uniqueDir, "partial_write_test.txt");

        assertFalse(storage.exists(path));
        assertThrows(MockException.class, () -> {
            try (ObjectWriter writer = storage.openWrite(path, WRITER_TIMEOUT_MILLIS)) {
                writer.stream().write("First chunk of data\n".getBytes(StandardCharsets.UTF_8));
                writer.stream().write("Second chunk of data\n".getBytes(StandardCharsets.UTF_8));
                writer.stream().write("Third chunk of data\n".getBytes(StandardCharsets.UTF_8));
                throw new MockException("Error during multi-chunk write");
            }
        });

        assertFalse(storage.exists(path), "no object may exist after an exception during a multi-chunk write");
        assertEquals(List.of(), storage.listObjects(PurePosixPrefix.from(uniqueDir)), "an aborted write must not appear in a listing");
    }

    /** While a writer is open and uncommitted, the object must be invisible - including to a caller that flushes and then abandons the writer. */
    public void testOpenWriteWithoutProperClose() throws IOException {
        String uniqueDir = "dir" + uniqueSuffix;
        PurePosixPath path = PurePosixPath.from(uniqueDir, "not_properly_closed.txt");

        assertFalse(storage.exists(path));
        ObjectWriter writer = storage.openWrite(path, WRITER_TIMEOUT_MILLIS);
        writer.stream().write("First chunk of data\n".getBytes(StandardCharsets.UTF_8));
        writer.stream().write("Second chunk of data\n".getBytes(StandardCharsets.UTF_8));
        writer.stream().write("Third chunk of data\n".getBytes(StandardCharsets.UTF_8));
        writer.stream().flush();
        writer.stream().flush();

        assertFalse(storage.exists(path), "an uncommitted write must not be visible");
        assertEquals(List.of(), storage.listObjects(PurePosixPrefix.from(uniqueDir)), "an uncommitted write must not appear in a listing");

        writer.close();

        assertFalse(storage.exists(path), "closing without committing must not publish the object");
        assertEquals(List.of(), storage.listObjects(PurePosixPrefix.from(uniqueDir)));
    }

    /**
     * Asserts that the failure injected by {@link FailingInputStream} reached the caller. A backend may rethrow it unchanged or wrap it with context - the S3 backends wrap it
     * in an {@code IOException} naming the key - so this checks the cause chain instead of the exception type. Asserting the type alone would fail a correct backend, and
     * asserting only {@code IOException} would pass a backend that failed for an unrelated reason.
     */
    private static void assertInjectedFailureReachedTheCaller(Executable action) {
        IOException thrown = assertThrows(IOException.class, action);
        for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
            if (cause instanceof MockException) {
                return;
            }
        }
        throw new AssertionError("Expected the injected MockException in the cause chain, but the write failed with: " + thrown, thrown);
    }

    private static void deleteRecursively(Path directory) throws IOException {
        try (Stream<Path> tree = Files.walk(directory)) {
            for (Path path : tree.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    /** Distinguishes a deliberately injected failure from a real one, so a test cannot pass on the wrong exception. */
    public static class MockException extends IOException {
        public MockException(String message) {
            super(message);
        }
    }

    /**
     * Serves {@code content} until the reader has taken {@code failAfterBytes} of it, then throws. Mirrors the Python test framework's {@code FailingStream}.
     */
    public static final class FailingInputStream extends InputStream {
        private final byte[] content;
        private final int failAfterBytes;
        private int position;

        public FailingInputStream(byte[] content, int failAfterBytes) {
            this.content = content;
            this.failAfterBytes = failAfterBytes;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            return read(one, 0, 1) == -1 ? -1 : Byte.toUnsignedInt(one[0]);
        }

        @Override
        public int read(byte[] destination, int offset, int length) throws IOException {
            if (position >= failAfterBytes) {
                throw new MockException("Simulated stream read failure after " + position + " bytes");
            }
            if (position >= content.length) {
                return -1;
            }
            int read = Math.min(length, content.length - position);
            System.arraycopy(content, position, destination, offset, read);
            position += read;
            return read;
        }
    }
}
