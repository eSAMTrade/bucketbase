package com.esamtrade.bucketbase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class S3BucketTest {

    private S3Bucket bucket;
    private IBucketTester tester;

    @BeforeAll
    public static void setUpClass() {

    }

    @BeforeEach
    public void setUp() {
        String accessKey = System.getenv("MINIO_ACCESS_KEY");
        String secretKey = System.getenv("MINIO_SECRET_KEY");
        bucket = new S3Bucket("https://minio.esamtrade.vlada.ro", accessKey, secretKey, "minio-dev-tests");
        tester = new IBucketTester(bucket);
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
    void testPutAndGetParquetObjectStream() throws IOException {
        tester.testPutAndGetParquetObjectStream();
    }

    @Test
    void testGetSize() throws IOException {
        tester.testGetSize();
    }

    @Test
    void getListObjects() throws IOException {
        tester.testListObjects();
    }

    @Test
    void getListObjectsWithOver1000keys() throws IOException {
        tester.testListObjectsWithOver1000keys();
    }

    @Test
    void shallowListObjects() throws IOException {
        tester.testShallowListObjects();
    }

    @Test
    void shallowListObjectsWithOver1000keys() throws IOException {
        tester.testShallowListObjectsWithOver1000keys();
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