package com.esamtrade.bucketbase;

import com.esamtrade.bucketbase.RangeSeekableInputStream.RangeReader;
import org.apache.commons.io.FilenameUtils;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

/**
 * An {@link IBucket} that stores every object as a file under a local directory. It mirrors the Python library's {@code FSBucket}, so a directory written by one language is
 * readable by the other.
 *
 * <p>Writes never touch the object path directly. {@code FSBucket} writes the body to a file under the {@value #BUCKETBASE_TMP_DIR_NAME} directory inside the root, then moves
 * that file onto the object path with a single atomic rename. A reader therefore sees either the previous object or the complete new one, and a failed or abandoned write leaves
 * no object behind. The temporary directory is created inside the root and is excluded from every listing; its name starts with {@code $}, which no valid object key may
 * contain.</p>
 *
 * <p>{@code FSBucket} rejects an object name that resolves outside the root, such as {@code dir/../../secret.txt}. S3 treats {@code ..} as an ordinary key component, but on a
 * filesystem the same key escapes the bucket, so this backend refuses it instead of reading or writing another directory.</p>
 *
 * <p>Several instances may share one root directory, and separate processes may too. Concurrent writers to the same key are safe: each writes its own temporary file, and the
 * last rename wins.</p>
 */
public class FSBucket implements IBucket {

    /**
     * Copy buffer for streaming writes; matches the 128 KiB default read-ahead of common Linux block devices.
     */
    public static final int BUFFER_SIZE = 128 * 1024;

    /**
     * Directory inside the root holding in-flight writes. The {@code $} makes it an invalid object key, so no listing can reach it.
     */
    public static final String BUCKETBASE_TMP_DIR_NAME = "$bucketbase.tmp";

    /**
     * How long {@link #putObjectStream} retries the final rename before giving up.
     */
    public static final long DEFAULT_RENAME_TIMEOUT_MILLIS = 5_000L;

    /**
     * Replaces the key separator in a temporary file name, so one flat directory holds every in-flight write. Also an invalid object-key character.
     */
    private static final String TEMP_SEP = "#";

    private static final long RENAME_RETRY_MIN_MILLIS = 50L;
    private static final long RENAME_RETRY_JITTER_MILLIS = 50L;

    private final Path root;
    private final long renameTimeoutMillis;

    /**
     * Opens a bucket rooted at {@code root}, creating the directory if it does not exist.
     *
     * @throws IOException if the root directory cannot be created
     */
    public FSBucket(Path root) throws IOException {
        this(root, DEFAULT_RENAME_TIMEOUT_MILLIS);
    }

    /**
     * @param root                the directory holding the objects; created if it does not exist
     * @param renameTimeoutMillis how long a write retries the final rename while another process holds the target open. Windows denies a rename over an open file; POSIX does
     *                            not, so this only matters there.
     * @throws IOException if the root directory cannot be created
     */
    public FSBucket(Path root, long renameTimeoutMillis) throws IOException {
        Objects.requireNonNull(root, "root");
        if (renameTimeoutMillis <= 0) {
            throw new IllegalArgumentException("renameTimeoutMillis must be positive, but got " + renameTimeoutMillis);
        }
        Files.createDirectories(root);
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("root must be a directory, but got " + root);
        }
        this.root = root.toAbsolutePath().normalize();
        this.renameTimeoutMillis = renameTimeoutMillis;
    }

    /**
     * The local directory backing this bucket. Not part of {@link IBucket}, but callers frequently need the on-disk location.
     */
    public Path getRoot() {
        return root;
    }

    @Override
    public void putObject(PurePosixPath name, byte[] content) throws IOException {
        Objects.requireNonNull(content, "content");
        putObjectStream(name, new ByteArrayInputStream(content));
    }

    @Override
    public void putObjectStream(PurePosixPath name, InputStream stream) throws IOException {
        Objects.requireNonNull(stream, "stream");
        String validatedName = S3Names.validateName(name);
        Path objectPath = resolveObjectPath(validatedName);
        Path temporaryPath = createTemporaryPath(validatedName);
        try {
            Files.createDirectories(temporaryPath.getParent());
            try (OutputStream sink = Files.newOutputStream(temporaryPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                byte[] buffer = new byte[BUFFER_SIZE];
                for (int read = stream.read(buffer); read != -1; read = stream.read(buffer)) {
                    sink.write(buffer, 0, read);
                }
            }
            renameIntoPlace(temporaryPath, objectPath);
        } catch (IOException | RuntimeException failure) {
            deleteQuietly(temporaryPath);
            if (failure instanceof NoSuchFileException missingPath) {
                throwIfWindowsPathTooLong(objectPath, missingPath);
            }
            throw failure;
        }
    }

    @Override
    public byte[] getObject(PurePosixPath name) throws IOException {
        Path path = resolveObjectPath(S3Names.validateName(name));
        if (!Files.isRegularFile(path)) {
            throw notFound(name);
        }
        try {
            return Files.readAllBytes(path);
        } catch (NoSuchFileException removedConcurrently) {
            throw notFound(name);
        }
    }

    @Override
    public SeekableInputStream getObjectStream(PurePosixPath name) throws IOException {
        Path path = resolveObjectPath(S3Names.validateName(name));
        if (!Files.isRegularFile(path)) {
            throw notFound(name);
        }
        FileChannel channel;
        try {
            channel = FileChannel.open(path, StandardOpenOption.READ);
        } catch (NoSuchFileException removedConcurrently) {
            throw notFound(name);
        }
        try {
            // Positional reads leave the channel position untouched, so RangeSeekableInputStream owns the position alone.
            RangeReader rangeReader = (offset, destination, destinationOffset, length) -> readFully(channel, name, offset, destination, destinationOffset, length);
            return new RangeSeekableInputStream(channel.size(), BUFFER_SIZE, rangeReader, channel);
        } catch (IOException | RuntimeException failure) {
            channel.close();
            throw failure;
        }
    }

    @Override
    public long getSize(PurePosixPath name) throws IOException {
        Path path = resolveObjectPath(S3Names.validateName(name));
        if (!Files.isRegularFile(path)) {
            throw notFound(name);
        }
        try {
            return Files.size(path);
        } catch (NoSuchFileException removedConcurrently) {
            throw notFound(name);
        }
    }

    @Override
    public List<PurePosixPath> listObjects(PurePosixPrefix prefix) throws IOException {
        String validatedPrefix = S3Names.validatePrefix(prefix);
        Path start = root.resolve(directoryPartOf(validatedPrefix));
        if (!Files.isDirectory(start)) {
            return List.of();
        }
        // Walking only the directory named by the prefix avoids reading the whole root for a narrow prefix.
        try (Stream<Path> tree = Files.walk(start)) {
            return tree.filter(Files::isRegularFile)
                    .map(this::toObjectKey)
                    .filter(key -> key.startsWith(validatedPrefix) && !isTemporaryArtifact(key))
                    .map(PurePosixPath::from)
                    .toList();
        }
    }

    @Override
    public ShallowListing shallowListObjects(PurePosixPrefix prefix) throws IOException {
        String validatedPrefix = S3Names.validatePrefix(prefix);
        Path start = root.resolve(directoryPartOf(validatedPrefix));
        if (!Files.isDirectory(start)) {
            return new ShallowListing(List.of(), List.of());
        }
        List<PurePosixPath> objects = new ArrayList<>();
        List<PurePosixPrefix> prefixes = new ArrayList<>();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(start)) {
            for (Path entry : entries) {
                String key = toObjectKey(entry);
                if (!key.startsWith(validatedPrefix) || isTemporaryArtifact(key)) {
                    continue;
                }
                if (Files.isRegularFile(entry)) {
                    objects.add(PurePosixPath.from(key));
                } else if (Files.isDirectory(entry)) {
                    prefixes.add(PurePosixPrefix.from(key + PurePosixPath.SEP));
                } else {
                    throw new IOException("Unexpected entry in the bucket directory, neither a file nor a directory: " + entry);
                }
            }
        }
        return new ShallowListing(objects, prefixes);
    }

    @Override
    public boolean exists(PurePosixPath name) {
        return Files.isRegularFile(resolveObjectPath(S3Names.validateName(name)));
    }

    /**
     * Deletes each named object and prunes the directories it leaves empty. Deleting a key that does not exist is not an error, consistent with S3.
     *
     * <p>Pruning is not atomic with the deletion: a concurrent writer may recreate a directory while this method removes another. The objects themselves are unaffected.</p>
     */
    @Override
    public List<DeleteError> removeObjects(List<PurePosixPath> names) {
        // Validate and resolve every name before deleting anything, so a rejected batch cannot be applied halfway.
        List<String> validatedNames = names.stream().map(S3Names::validateName).toList();
        List<Path> paths = validatedNames.stream().map(this::resolveObjectPath).toList();

        List<DeleteError> errors = new ArrayList<>();
        for (int i = 0; i < paths.size(); i++) {
            Path path = paths.get(i);
            try {
                Files.deleteIfExists(path);
            } catch (IOException failure) {
                // A local filesystem reports no provider error code, so the exception type names the failure.
                errors.add(new DeleteError(failure.getClass().getSimpleName(), failure.getMessage(), validatedNames.get(i)));
                continue;
            }
            removeEmptyParentDirectories(path);
        }
        return errors;
    }

    private Path createTemporaryPath(String validatedName) {
        String flatName = validatedName.replace(PurePosixPath.SEP, TEMP_SEP);
        String fileName = flatName + "@" + System.nanoTime() + "-" + Thread.currentThread().threadId() + ".tmp";
        return root.resolve(BUCKETBASE_TMP_DIR_NAME).resolve(fileName);
    }

    /**
     * Moves a completed temporary file onto its object path, retrying while the target is denied.
     *
     * <p>Windows refuses to replace a file another process holds open, and reports it as {@link AccessDeniedException}. Retrying makes a write succeed as soon as the reader
     * finishes instead of failing the caller.</p>
     */
    private void renameIntoPlace(Path temporaryPath, Path objectPath) throws IOException {
        Files.createDirectories(objectPath.getParent());
        long deadlineNanos = System.nanoTime() + renameTimeoutMillis * 1_000_000L;
        while (true) {
            try {
                Files.move(temporaryPath, objectPath, StandardCopyOption.ATOMIC_MOVE);
                return;
            } catch (AccessDeniedException denied) {
                if (System.nanoTime() >= deadlineNanos) {
                    throw new IOException("Timed out after " + renameTimeoutMillis + " ms renaming " + temporaryPath + " to " + objectPath, denied);
                }
                sleepBeforeRenameRetry(temporaryPath, objectPath);
            }
        }
    }

    private static void sleepBeforeRenameRetry(Path temporaryPath, Path objectPath) throws IOException {
        try {
            // Jitter keeps concurrent writers of the same key from retrying in lockstep.
            Thread.sleep(RENAME_RETRY_MIN_MILLIS + ThreadLocalRandom.current().nextLong(RENAME_RETRY_JITTER_MILLIS));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while renaming " + temporaryPath + " to " + objectPath, interrupted);
        }
    }

    private static void readFully(FileChannel channel, PurePosixPath name, long offset, byte[] destination, int destinationOffset, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(destination, destinationOffset, length);
        long position = offset;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position);
            if (read < 0) {
                throw new IOException("Object " + name + " was truncated while being read: " + buffer.remaining() + " bytes missing at offset " + position);
            }
            position += read;
        }
    }

    /**
     * The directory part of a prefix: everything up to and including its last separator, or {@code ""} when it has none.
     */
    private static String directoryPartOf(String validatedPrefix) {
        return validatedPrefix.substring(0, validatedPrefix.lastIndexOf('/') + 1);
    }

    private String toObjectKey(Path path) {
        return FilenameUtils.separatorsToUnix(root.relativize(path).toString());
    }

    private static boolean isTemporaryArtifact(String key) {
        return key.equals(BUCKETBASE_TMP_DIR_NAME) || key.startsWith(BUCKETBASE_TMP_DIR_NAME + PurePosixPath.SEP);
    }

    private Path resolveObjectPath(String validatedName) {
        Path resolved = root.resolve(validatedName).normalize();
        if (!resolved.startsWith(root) || resolved.equals(root)) {
            throw new IllegalArgumentException("Object name resolves outside the bucket root: " + validatedName);
        }
        return resolved;
    }

    private void removeEmptyParentDirectories(Path objectPath) {
        for (Path directory = objectPath.getParent(); directory != null && !directory.equals(root) && directory.startsWith(root); directory = directory.getParent()) {
            try {
                Files.delete(directory);
            } catch (IOException stillInUseOrAlreadyGone) {
                return;
            }
        }
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // The write already failed; reporting the cleanup failure instead would hide its cause.
        }
    }

    /**
     * Rewrites a missing-path failure into an explanation when Windows path length is the likely cause. A deep bucket root plus a long key exceeds the 260-character limit, and
     * the filesystem reports only that the path does not exist.
     */
    private static void throwIfWindowsPathTooLong(Path objectPath, IOException cause) throws IOException {
        if (!System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win")) {
            return;
        }
        if (objectPath.toString().length() < WINDOWS_MAX_PATH - MINIO_PATH_TEMP_SUFFIX_LEN) {
            return;
        }
        throw new IOException(
                "Reduce the bucket root path length. Windows limits a path to " + WINDOWS_MAX_PATH + " characters. "
                        + "More details here: https://docs.python.org/3/using/windows.html#removing-the-max-path-limitation", cause);
    }

    private static FileNotFoundException notFound(PurePosixPath name) {
        return new FileNotFoundException("Object " + name + " not found in FSBucket");
    }
}
