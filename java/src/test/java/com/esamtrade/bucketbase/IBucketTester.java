package com.esamtrade.bucketbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IBucketTester {

    private static final List<String> INVALID_PREFIXES = List.of("/", "/dir", "star*1", "dir1/a\\file.txt", "at@gmail", "sharp#1", "dollar$1", "comma,");
    private final BaseBucket storage;
    private final String uniqueSuffix;

    public IBucketTester(BaseBucket storage) {
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
        assertArrayEquals(retrievedContent, sContent.getBytes(StandardCharsets.UTF_8));

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
        assertArrayEquals(retrievedContent, sContent.getBytes(StandardCharsets.UTF_8));

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
        try (ObjectStream file = storage.getObjectStream(path)) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(file.getStream())))) {
                String[] result = new String[3];
                for (int i = 0; i < 3; i++) {
                    result[i] = reader.readLine();
                }
                assertArrayEquals(new String[]{"Test", "content", null}, result);
            }
        }

        // String path
        path = PurePosixPath.from(uniqueDir, "file1.bin");
        try (ObjectStream file = storage.getObjectStream(path)) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(file.getStream())))) {
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

    public void testPutAndGetParquetObjectStream() throws IOException {
        MessageType schema = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("int_field")
                .named("TestSchema");

        String uniqueDir = "dir" + uniqueSuffix;

        // Binary content
        PurePosixPath path = PurePosixPath.from(uniqueDir, "file1.parquet");
        int totalRows = 3;
        InputStream inputStream = generateParquetOutput(totalRows);
        storage.putObjectStream(path, inputStream);

        int count = readRowsCountFromParquet(path, schema);
        assertEquals(totalRows, count);
    }

    public void testPutAndGetMultiUploadLargeParquetObjectStream() throws IOException {
        // This test is intended to generate ~6MB parquet file, so triggering multiple multipart uploads for S3
        MessageType schema = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("int_field")
                .named("TestSchema");

        String uniqueDir = "dir" + uniqueSuffix;

        // Binary content
        PurePosixPath path = PurePosixPath.from(uniqueDir, "file20.parquet");
        int totalRows = 1_500_000;
        InputStream inputStream = generateParquetOutput(totalRows);
        storage.putObjectStream(path, inputStream);

        int count = readRowsCountFromParquet(path, schema);
        assertEquals(totalRows, count);
    }

    private int readRowsCountFromParquet(PurePosixPath path, MessageType schema) throws IOException {
        ObjectStream objectStream = storage.getObjectStream(path);
        assertTrue(objectStream.getStream().markSupported());
        long size = storage.getSize(path);

        InputFile inFile = new ParquetUtils.StreamInputFile(objectStream, size);
        int count = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(inFile)) {
            // get actual file schema
            var metadata = reader.getFooter().getFileMetaData();
            MessageType fileSchema = metadata.getSchema();
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                MessageColumnIO columnIO = new ColumnIOFactory()
                        .getColumnIO(schema, fileSchema);
                RecordReader<Group> rr = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (int i = 0, rows = (int) pages.getRowCount(); i < rows; i++) {
                    Group g = rr.read();
                    assertEquals(count, g.getInteger("int_field", 0));
                    count++;
                }
            }
        }
        return count;
    }

    public void testGetSize() throws IOException {
        String uniqueDir = "dir" + uniqueSuffix;

        // Binary content
        PurePosixPath path = PurePosixPath.from(uniqueDir, "file1.bin");
        byte[] bContent = "Test\ncontent".getBytes();
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bContent);
        storage.putObjectStream(path, byteStream);

        long size = storage.getSize(path);
        assertEquals(bContent.length, size);
        assertThrows(FileNotFoundException.class, () -> storage.getSize(new PurePosixPath(uniqueDir, "inexistent.txt")));
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
        final String PATH_WITH_2025_KEYS = "test-dir-with-2025-keys/";
        var pathWith2025Keys = new PurePosixPath(PATH_WITH_2025_KEYS);
        List<PurePosixPath> existingKeys = storage.listObjects(pathWith2025Keys);
        if (existingKeys.isEmpty()) {
            // Create the directory and add 2025 files
            ForkJoinPool customThreadPool = new ForkJoinPool(50);
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
        return pathWith2025Keys;
    }

    public void testShallowListObjectsWithOver1000keys() throws IOException {
        // Check if PATH_WITH_2025_KEYS exists
        var pathWith2025Keys = ensureDirWith2025Keys();

        ShallowListing objects = storage.shallowListObjects(pathWith2025Keys);
        assertEquals(2025, objects.getObjects().size());
        assertEquals(0, objects.getPrefixes().size());
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
        assertIterableEquals(expectedObjects, objects.getObjects());
        assertIterableEquals(expectedPrefixes, objects.getPrefixes());

        ShallowListing shallowListing = storage.shallowListObjects(new PurePosixPath(uniqueDir));
        expectedObjects = List.of(new PurePosixPath(uniqueDir + "file1.txt"));
        expectedPrefixes = List.of(PurePosixPath.from(uniqueDir + "/"));
        assertInstanceOf(List.class, shallowListing.getObjects());
        assertInstanceOf(List.class, shallowListing.getPrefixes());
        assertIterableEquals(expectedObjects, shallowListing.getObjects());
        assertIterableEquals(expectedPrefixes, shallowListing.getPrefixes());

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
        assertInstanceOf(List.class, result);
        assertEquals(List.of(), result);
        assertFalse(storage.exists(path1));
        assertFalse(storage.exists(path2));
        assertThrows(FileNotFoundException.class, () -> storage.getObject(new PurePosixPath(uniqueDir + "/file1.txt")));
        assertThrows(IllegalArgumentException.class, () -> storage.removeObjects(List.of(new PurePosixPath(uniqueDir + "/"))));

        // Check that the leftover empty directories are also removed, but the bucket may contain leftovers from the other test runs
        ShallowListing shallowListing = storage.shallowListObjects(new PurePosixPath(""));
        List<PurePosixPath> prefixes = shallowListing.getPrefixes();
        List<String> prefixStrings = prefixes.stream().map(PurePosixPath::toString).toList();
        assertFalse(prefixStrings.contains(uniqueDir + "/"));
    }

    private InputStream generateParquetOutput(int rows) throws IOException {
        MessageType schema = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("int_field")
                .named("TestSchema");

        // enable writing
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);

        int pipeBuffer = 4 * 1024 * 1024; // 4 MB internal buffer
        PipedInputStream inputStreamToReturn = new PipedInputStream(pipeBuffer);
        PipedOutputStream pipedOutputStream = new PipedOutputStream(inputStreamToReturn);

        // wrap pipe in our PositionOutputStream
        ParquetUtils.PositionOutputStreamWrapper posOutStream = new ParquetUtils.PositionOutputStreamWrapper(pipedOutputStream);
        ParquetUtils.OutputFileWrapper OutputFileWrapper = new ParquetUtils.OutputFileWrapper(posOutStream);

        // start a thread to write Parquet into the pipe
        Thread writerThread = new Thread(() -> {
            try (ParquetWriter<Group> writer = ExampleParquetWriter
                    .builder(OutputFileWrapper)
                    .withConf(conf)
                    .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)
                    .withType(schema)
                    .build()) {

                SimpleGroupFactory factory = new SimpleGroupFactory(schema);
                for (int i = 0; i < rows; i++) {
                    writer.write(factory.newGroup().append("int_field", i));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        writerThread.start();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return inputStreamToReturn;
    }
}
