package com.esamtrade.bucketbase;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link StreamPipe} documents bounded capacity, i.e. a slow reader back-pressures the writer
 * instead of letting data pile up in memory. That bound must be expressed in <em>bytes</em>: if it
 * counts write calls instead, a single large {@code write(byte[])} - which is exactly what a
 * Parquet writer flushing a row group does - sails straight past it.
 */
class StreamPipeTest {

    @Test
    void aSingleLargeWriteIsBackPressuredUntilTheReaderDrains() throws Exception {
        StreamPipe pipe = new StreamPipe();
        InputStream in = pipe.inputStream();
        OutputStream out = pipe.outputStream();

        byte[] payload = payload(8 * 1024 * 1024);
        CountDownLatch writeReturned = new CountDownLatch(1);
        AtomicReference<Throwable> writeFailure = new AtomicReference<>();

        Thread writer = new Thread(() -> {
            try {
                out.write(payload);        // one big call, no reader consuming yet
                pipe.finish();
            } catch (Throwable t) {
                writeFailure.set(t);
            } finally {
                writeReturned.countDown();
            }
        }, "test-writer");
        writer.setDaemon(true);
        writer.start();

        assertFalse(writeReturned.await(500, TimeUnit.MILLISECONDS),
                "an 8 MiB write completed with nothing consuming it, so the pipe buffered it all "
                        + "- capacity is bounded by call count, not by bytes");

        byte[] received = in.readAllBytes();
        assertTrue(writeReturned.await(5, TimeUnit.SECONDS), "the writer never finished after the reader drained");
        assertNull(writeFailure.get(), String.valueOf(writeFailure.get()));
        assertArrayEquals(payload, received, "every byte must survive the chunking");
    }

    @Test
    void smallWritesRoundTripInOrder() throws Exception {
        StreamPipe pipe = new StreamPipe();
        InputStream in = pipe.inputStream();

        Thread writer = new Thread(() -> {
            try (OutputStream out = pipe.outputStream()) {
                for (int i = 0; i < 1_000; i++) {
                    out.write(("line-" + i + "\n").getBytes());
                }
                pipe.finish();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, "test-writer");
        writer.setDaemon(true);
        writer.start();

        String received = new String(in.readAllBytes());
        writer.join(5_000);
        assertTrue(received.startsWith("line-0\n"));
        assertTrue(received.endsWith("line-999\n"));
        assertEquals(1_000, received.lines().count());
    }

    @Test
    void abortMakesTheReaderFail() throws Exception {
        StreamPipe pipe = new StreamPipe();
        InputStream in = pipe.inputStream();
        pipe.outputStream().write("partial".getBytes());
        pipe.abort(new IllegalStateException("caller gave up"));

        IOException failure = org.junit.jupiter.api.Assertions.assertThrows(IOException.class, in::readAllBytes);
        assertTrue(failure.getMessage().contains("aborted"), failure.getMessage());
    }

    private static byte[] payload(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 251);
        }
        return data;
    }
}
