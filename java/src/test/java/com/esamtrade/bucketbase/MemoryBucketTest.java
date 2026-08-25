package com.esamtrade.bucketbase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class MemoryBucketTest {
    MemoryBucket storage;
    IBucketTester tester;

    @BeforeEach
    void setUp() {
        this.storage = new MemoryBucket();
        this.tester = new IBucketTester(storage);
    }

    @AfterEach
    void tearDown() throws IOException {
        tester.cleanup();
    }

    @Test
    public void testPutAndGetObject() throws IOException {
        tester.testPutAndGetObject();
    }

    @Test
    void putObjectAndGetObjectStream() throws IOException {
        tester.testPutAndGetObjectStream();
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
    }

    @Test
    void streamingFailureAtomicity() throws IOException {
        tester.testStreamingFailureAtomicity();
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
}
