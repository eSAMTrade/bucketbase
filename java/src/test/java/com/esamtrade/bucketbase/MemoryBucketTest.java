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
    void testPutAndGetParquetObjectStream() throws IOException {
        tester.testPutAndGetParquetObjectStream();
    }

    @Test
    void testGetSize() throws IOException {
        tester.testGetSize();
    }

    @Test
    void putObjectAndGetObjectStream() throws IOException {
        tester.testPutAndGetObjectStream();
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

}