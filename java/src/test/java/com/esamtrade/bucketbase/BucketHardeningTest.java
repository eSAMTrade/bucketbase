package com.esamtrade.bucketbase;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Backend-agnostic correctness fixes, exercised against {@link MemoryBucket} because they hold
 * for every backend and need no network.
 */
class BucketHardeningTest {

    private final MemoryBucket bucket = new MemoryBucket();

    @Test
    void getObjectReturnsAnIndependentCopy() throws IOException {
        PurePosixPath name = PurePosixPath.from("dir/file.bin");
        byte[] original = {1, 2, 3};
        bucket.putObject(name, original);

        // Mutating the array we passed in must not change stored bytes.
        original[0] = 99;
        assertArrayEquals(new byte[]{1, 2, 3}, bucket.getObject(name));

        // Mutating the array we got back must not change stored bytes either.
        byte[] fetched = bucket.getObject(name);
        fetched[0] = 42;
        assertArrayEquals(new byte[]{1, 2, 3}, bucket.getObject(name));
    }

    @Test
    void removeObjectsIsAtomicOnInvalidName() throws IOException {
        PurePosixPath valid = PurePosixPath.from("dir/keep.bin");
        bucket.putObject(valid, new byte[]{1});

        // A batch containing an invalid name must be rejected wholesale, not applied halfway.
        assertThrows(IllegalArgumentException.class,
                () -> bucket.removeObjects(List.of(valid, new PurePosixPath("bad*"))));
        assertTrue(bucket.exists(valid), "a rejected batch must not have deleted the valid key");
    }

    @Test
    void unicodeAndApostropheNamesAreAccepted() throws IOException {
        // Matches the Python library's allowed character set, so keys interoperate across languages.
        for (String key : List.of("dir/café.txt", "dir/файл.bin", "dir/o'brien.txt", "dir/日本語.dat")) {
            PurePosixPath name = PurePosixPath.from(key);
            bucket.putObject(name, key.getBytes());
            assertArrayEquals(key.getBytes(), bucket.getObject(name));
        }
    }

    @Test
    void invalidNamesAreStillRejected() {
        for (String key : List.of("star*1", "at@gmail", "sharp#1", "comma,", "back\\slash")) {
            assertThrows(IllegalArgumentException.class,
                    () -> bucket.putObject(PurePosixPath.from(key), new byte[0]),
                    "expected rejection of: " + key);
        }
    }

    @Test
    void removeObjectsReportsNoErrorsForMissingKeys() throws IOException {
        // Deleting a key that does not exist is not an error, consistent with S3.
        List<DeleteError> errors = bucket.removeObjects(List.of(PurePosixPath.from("dir/ghost.bin")));
        assertEquals(List.of(), errors);
    }

    @Test
    void copyPrefixWithEmptySourceIsANoOp() throws IOException {
        MemoryBucket dst = new MemoryBucket();
        // Must not throw (the old code created a zero-size thread pool).
        bucket.copyPrefix(dst, PurePosixPrefix.from("nothing-here/"), PurePosixPrefix.from("out/"), 4);
        assertTrue(dst.listObjects(PurePosixPrefix.from("out/")).isEmpty());
    }

    @Test
    void copyPrefixCopiesAllObjects() throws IOException {
        bucket.putObject(PurePosixPath.from("src/a.txt"), "a".getBytes());
        bucket.putObject(PurePosixPath.from("src/nested/b.txt"), "b".getBytes());
        MemoryBucket dst = new MemoryBucket();

        // threads > 1 exercises the executor path.
        bucket.copyPrefix(dst, PurePosixPrefix.from("src/"), PurePosixPrefix.from("dst/"), 3);

        assertArrayEquals("a".getBytes(), dst.getObject(PurePosixPath.from("dst/a.txt")));
        assertArrayEquals("b".getBytes(), dst.getObject(PurePosixPath.from("dst/nested/b.txt")));
        assertEquals(2, dst.listObjects(PurePosixPrefix.from("dst/")).size());
    }

    @Test
    void movePrefixRemovesTheSource() throws IOException {
        bucket.putObject(PurePosixPath.from("src/a.txt"), "a".getBytes());
        MemoryBucket dst = new MemoryBucket();

        bucket.movePrefix(dst, PurePosixPrefix.from("src/"), PurePosixPrefix.from("dst/"), 1);

        assertArrayEquals("a".getBytes(), dst.getObject(PurePosixPath.from("dst/a.txt")));
        assertTrue(bucket.listObjects(PurePosixPrefix.from("src/")).isEmpty());
    }

    @Test
    void copyObjectFromStreamsBetweenBuckets() throws IOException {
        bucket.putObject(PurePosixPath.from("src.bin"), "payload".getBytes());
        MemoryBucket dst = new MemoryBucket();

        dst.copyObjectFrom(bucket, PurePosixPath.from("src.bin"), PurePosixPath.from("dst.bin"));

        assertArrayEquals("payload".getBytes(), dst.getObject(PurePosixPath.from("dst.bin")));
    }

    @Test
    void memoryBucketCloseIsANoOp() throws IOException {
        bucket.putObject(PurePosixPath.from("a.bin"), new byte[]{1});
        bucket.close();
        // Still usable: close() releases resources but MemoryBucket holds none.
        assertTrue(bucket.exists(PurePosixPath.from("a.bin")));
    }

    @Test
    void noArgumentListingsUseTheEmptyPrefix() throws IOException {
        bucket.putObject(PurePosixPath.from("root.bin"), new byte[]{1});
        bucket.putObject(PurePosixPath.from("dir/nested.bin"), new byte[]{2});

        assertEquals(2, bucket.listObjects().size());
        assertEquals(List.of(PurePosixPath.from("root.bin")), bucket.shallowListObjects().objects());
        assertEquals(List.of(PurePosixPrefix.from("dir/")), bucket.shallowListObjects().prefixes());
    }

    @Test
    void appendOnlyRejectsMutations() throws IOException {
        LockRecordingAppendOnly appendOnly = new LockRecordingAppendOnly(bucket);
        PurePosixPath name = PurePosixPath.from("dir/once.bin");

        appendOnly.putObject(name, "first".getBytes());
        assertArrayEquals("first".getBytes(), appendOnly.getObject(name));
        assertTrue(appendOnly.wasLocked());

        assertThrows(UnsupportedOperationException.class, () -> appendOnly.removeObjects(List.of(name)));
        assertThrows(UnsupportedOperationException.class, () -> appendOnly.removePrefix(PurePosixPrefix.from("dir/")));
        // movePrefix must fail fast rather than copy-then-fail.
        MemoryBucket dst = new MemoryBucket();
        assertThrows(UnsupportedOperationException.class,
                () -> appendOnly.movePrefix(
                        dst, PurePosixPrefix.from("dir/"), PurePosixPrefix.from("out/"), 1));
        assertFalse(dst.exists(PurePosixPath.from("out/once.bin")), "movePrefix must not have copied anything");
    }

    /** Minimal concrete append-only bucket that records whether a lock was taken. */
    private static final class LockRecordingAppendOnly extends AbstractAppendOnlySynchronizedBucket {
        private volatile boolean locked;

        LockRecordingAppendOnly(IBucket base) {
            super(base);
        }

        boolean wasLocked() {
            return locked;
        }

        @Override
        protected void lockObject(PurePosixPath name) {
            locked = true;
        }

        @Override
        protected void unlockObject(PurePosixPath name) {
        }
    }
}
