package com.esamtrade.bucketbase;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class FileLockForPathTest {

    @Test
    void acquire() throws Exception {
        // Create a temporary file for testing
        Path tempFile = Files.createTempFile("locktest", ".tmp");

        // Set up thread coordination objects
        CountDownLatch thread1Acquired = new CountDownLatch(1);
        CountDownLatch thread2Attempt = new CountDownLatch(1);
        CountDownLatch thread1Released = new CountDownLatch(1);

        AtomicBoolean thread1Success = new AtomicBoolean(false);
        AtomicBoolean thread2FirstAttemptSuccess = new AtomicBoolean(false);
        AtomicBoolean thread2SecondAttemptSuccess = new AtomicBoolean(false);

        long DEFAULT_TIMEOUT = 5000L;
        // Thread 1: First to acquire lock
        Thread thread1 = new Thread(() -> {
            try (FileLockForPath lock = new FileLockForPath(tempFile)) {
                thread1Success.set(lock.tryLock(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS));
                thread1Acquired.countDown(); // Signal that thread1 has acquired the lock

                // Wait for thread2 to attempt acquiring the lock
                thread2Attempt.await();

                // Hold the lock for a moment
                Thread.sleep(500);

                // Release the lock
                lock.unlock();
                thread1Released.countDown(); // Signal that thread1 has released the lock
            } catch (Exception e) {
                fail("Thread 1 encountered exception: " + e);
            }
        });

        // Thread 2: Try to acquire while thread1 holds lock, then try again after release
        Thread thread2 = new Thread(() -> {
            try {
                // Wait for thread1 to acquire the lock
                thread1Acquired.await();

                // First attempt - should fail or timeout
                try (FileLockForPath lock = new FileLockForPath(tempFile)) {
                    thread2FirstAttemptSuccess.set(lock.tryLock());
                }
                thread2Attempt.countDown(); // Signal that thread2 has attempted to acquire

                // Wait for thread1 to release lock
                thread1Released.await();

                // Second attempt - should succeed
                try (FileLockForPath lock = new FileLockForPath(tempFile)) {
                    thread2SecondAttemptSuccess.set(lock.tryLock());

                    // Hold the lock briefly
                    Thread.sleep(100);
                }
            } catch (Exception e) {
                fail("Thread 2 encountered exception: " + e);
            }
        });

        // Start the threads and wait for completion
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        // Verify the test results
        assertTrue(thread1Success.get(), "Thread 1 should acquire the lock successfully");
        assertFalse(thread2FirstAttemptSuccess.get(), "Thread 2 should fail to acquire the lock while Thread 1 holds it");
        assertTrue(thread2SecondAttemptSuccess.get(), "Thread 2 should acquire the lock after Thread 1 releases it");

        // Clean up
        Files.deleteIfExists(tempFile);
        Files.deleteIfExists(Path.of(tempFile.toString() + ".lock"));
    }

    @Test
    void testFileLockWithTwoThreads() throws Exception {
        // Create a temporary directory and test file
        Path tempDir = Files.createTempDirectory("locktest");
        Path testFile = tempDir.resolve("testfile.txt");

        // Create two instances for the same file (they use a lock file with ".lock" appended)
        FileLockForPath lock1 = new FileLockForPath(testFile);
        FileLockForPath lock2 = new FileLockForPath(testFile);

        AtomicBoolean thread2AcquiredAfterDelay = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        Thread thread1 = new Thread(() -> {
            try {
                // First thread acquires the lock (should succeed immediately)
                lock1.lock();
                // Signal that lock1 is held so thread2 can start trying to acquire
                latch.countDown();
                // Hold the lock for 500ms
                Thread.sleep(500);
                lock1.unlock();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        Thread thread2 = new Thread(() -> {
            try {
                // Wait until thread1 has acquired the lock
                latch.await();
                long start = System.currentTimeMillis();
                // Retry loop: if an OverlappingFileLockException occurs (since locks in one JVM conflict),
                // catch it, wait a bit and retry until the lock becomes available.
                lock2.lock();
                long elapsed = System.currentTimeMillis() - start;
                // We expect that thread2 didn't get the lock until after ~500ms, so allow a little slack
                if (elapsed >= 400) {
                    thread2AcquiredAfterDelay.set(true);
                }
                lock2.unlock();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        assertTrue(thread2AcquiredAfterDelay.get(),
                "Thread2 should acquire the lock after thread1 releases it.");

        // Cleanup temporary directory and files
        Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }
}