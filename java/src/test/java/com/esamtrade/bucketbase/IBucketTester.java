package com.esamtrade.bucketbase;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;
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
    private final IBucket storage;
    private final String uniqueSuffix;
    private final String PATH_WITH_2025_KEYS = "test-dir-with-2025-keys/";

    public IBucketTester(IBucket storage) {
        this.storage = storage;
        // Generate a unique suffix to be used in the names of dirs and files
        this.uniqueSuffix = String.format("%08d", System.currentTimeMillis() % 100_000_000);
    }

    public void cleanup() throws IOException {
        storage.removePrefix(PurePosixPath.from("dir" + uniqueSuffix));
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

        List<PurePosixPath> objects = storage.listObjects(PurePosixPath.from(uniqueDir)).stream().sorted().toList();
        List<PurePosixPath> expectedObjects = List.of(
                PurePosixPath.from(uniqueDir, "dir2/file2.txt"),
                PurePosixPath.from(uniqueDir, "file1.txt"),
                PurePosixPath.from(uniqueDir + "file1.txt")
                                                     );
        assertEquals(expectedObjects, objects);

        objects = storage.listObjects(PurePosixPath.from(uniqueDir + "/")).stream().sorted().toList();
        expectedObjects = List.of(
                PurePosixPath.from(uniqueDir, "dir2/file2.txt"),
                PurePosixPath.from(uniqueDir, "file1.txt")
                                 );
        assertEquals(expectedObjects, objects);

        // Invalid Prefix cases
        for (String prefix : INVALID_PREFIXES) {
            assertThrows(IllegalArgumentException.class, () -> storage.listObjects(PurePosixPath.from(prefix)), "Invalid prefix: " + prefix);
        }
    }

    public void testListObjectsWithOver1000keys() throws IOException {
        // Check if PATH_WITH_2025_KEYS exists
        var pathWith2025Keys = ensureDirWith2025Keys();

        List<PurePosixPath> objects = storage.listObjects(pathWith2025Keys);
        assertEquals(2025, objects.size());
    }

    private PurePosixPath ensureDirWith2025Keys() throws IOException {
        var pathWith2025Keys = new PurePosixPath(PATH_WITH_2025_KEYS);
        List<PurePosixPath> existingKeys = storage.listObjects(pathWith2025Keys);
        if (existingKeys.isEmpty()) {
            // Create the directory and add 2025 files
            try (ForkJoinPool customThreadPool = new ForkJoinPool(50)) {
                try {
                    customThreadPool.submit(() ->
                                    IntStream.range(0, 2025).parallel().forEach(i -> {
                                        try {
                                            var path = pathWith2025Keys.join("file" + i + ".txt");
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
        assertEquals(2025, objects.objects().size());
        assertEquals(0, objects.prefixes().size());
    }

    public void testShallowListObjects() throws IOException {
        String uniqueDir = "dir" + uniqueSuffix;
        storage.putObject(new PurePosixPath(uniqueDir + "/file1.txt"), "Content 1".getBytes());
        storage.putObject(new PurePosixPath(uniqueDir + "/dir2/file2.txt"), "Content 2".getBytes());
        storage.putObject(new PurePosixPath(uniqueDir + "file1.txt"), "Content 3".getBytes());

        assertThrows(IllegalArgumentException.class, () -> storage.shallowListObjects(new PurePosixPath("/")));
        assertThrows(IllegalArgumentException.class, () -> storage.shallowListObjects(new PurePosixPath("/d")));

        ShallowListing objects = storage.shallowListObjects(new PurePosixPath(uniqueDir + "/"));
        List<PurePosixPath> expectedObjects = List.of(PurePosixPath.from(uniqueDir + "/file1.txt"));
        List<PurePosixPath> expectedPrefixes = List.of(PurePosixPath.from(uniqueDir + "/dir2/"));
        assertIterableEquals(expectedObjects, objects.objects());
        assertIterableEquals(expectedPrefixes, objects.prefixes());

        ShallowListing shallowListing = storage.shallowListObjects(new PurePosixPath(uniqueDir));
        expectedObjects = List.of(new PurePosixPath(uniqueDir + "file1.txt"));
        expectedPrefixes = List.of(PurePosixPath.from(uniqueDir + "/"));
        assertIterableEquals(expectedObjects, shallowListing.objects());
        assertIterableEquals(expectedPrefixes, shallowListing.prefixes());

        // Invalid Prefix cases
        for (String prefix : INVALID_PREFIXES) {
            assertThrows(IllegalArgumentException.class, () -> storage.shallowListObjects(new PurePosixPath(prefix)));
        }
    }

    public void testExists() throws IOException {
        String uniqueDir = "dir" + uniqueSuffix;
        PurePosixPath path = new PurePosixPath(uniqueDir + "/file.txt");
        storage.putObject(path, "Content".getBytes());

        assertTrue(storage.exists(path));
        assertFalse(storage.exists(new PurePosixPath(uniqueDir)));
        assertThrows(IllegalArgumentException.class, () -> storage.exists(new PurePosixPath(uniqueDir + "/")));
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
        assertThrows(IllegalArgumentException.class, () -> storage.removeObjects(List.of(new PurePosixPath(uniqueDir + "/"))));

        // Check that the leftover empty directories are also removed, but the bucket may contain leftovers from the other test runs
        ShallowListing shallowListing = storage.shallowListObjects(new PurePosixPath(""));
        List<PurePosixPath> prefixes = shallowListing.prefixes();
        assertFalse(prefixes.contains(uniqueDir + "/"));
    }
}
