package com.esamtrade.bucketbase;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class FileLockForPath implements Lock, AutoCloseable {
    private static final ConcurrentHashMap<Path, Thread> LOCKED_PATHS = new ConcurrentHashMap<>();

    private final Path lockFilePath;
    private FileChannel channel;
    private FileLock lock;

    /**
     * Creates FileLock for a Path destination by creating a lock file with the same name extended with .lock
     *
     * @param path The path to lock
     */
    public FileLockForPath(Path path) {
        this.lockFilePath = Path.of(path.toString() + ".lock");
    }

    /**
     * Acquires the lock
     *
     * @return true if the lock was acquired, false otherwise
     * @throws IOException if an I/O error occurs
     */
    private boolean acquire(long timeout, TimeUnit unit) throws IOException {
        long timeOutMillis = unit.toMillis(timeout);
        // Check if this JVM already holds the lock for this path
        Thread currentOwner = LOCKED_PATHS.get(lockFilePath);
        long startTime = System.currentTimeMillis();
        if (currentOwner == Thread.currentThread()) {
            // Already locked by current thread
            return true;
        } else if (currentOwner != null) {
            if (timeout == -1) {
                // Already locked by another thread in this JVM, wait indefinitely
                timeOutMillis = Long.MAX_VALUE;
            }
            // Already locked by another thread in this JVM, wait for timeout

            while (LOCKED_PATHS.containsKey(lockFilePath)) {
                if (System.currentTimeMillis() - startTime > timeOutMillis) {
                    return false;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }

        // Create parent directories if they don't exist
        Files.createDirectories(lockFilePath.getParent());

        // Open or create the lock file
        channel = FileChannel.open(
                lockFilePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ);

        // Try to acquire the lock
        while (true) {
            try {
                lock = channel.tryLock();
                if (lock != null) {
                    LOCKED_PATHS.put(lockFilePath, Thread.currentThread());
                    return true;
                }
            } catch (IOException e) {
                // Failed to acquire lock
            }

            // Check for timeout
            if (System.currentTimeMillis() - startTime > timeOutMillis) {
                return false;
            }

            try {
                Thread.sleep(100); // Wait a bit before retrying
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }


    @Override
    public void close() throws IOException {
        unlock();
    }

    @Override
    public void lock() {
        try {
            acquire(-1, TimeUnit.MILLISECONDS);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean tryLock() {
        try {
            return acquire(0, TimeUnit.MILLISECONDS);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        try {
            return acquire(time, unit);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void unlock() {
        LOCKED_PATHS.remove(lockFilePath);
        try {
            if (lock != null) {
                lock.release();
                lock = null;
            }
            if (channel != null) {
                channel.close();
                channel = null;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Condition newCondition() {
        return null;
    }
}