package com.esamtrade.bucketbase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class S3BucketSDKv1Test {
    private S3BucketSDKv1 bucket;
    private IBucketTester tester;

    @BeforeAll
    public static void setUpClass() {

    }

    @BeforeEach
    public void setUp() {
        String accessKey = System.getenv("MINIO_ACCESS_KEY");
        String secretKey = System.getenv("MINIO_SECRET_KEY");
        bucket = new S3BucketSDKv1("https://minio.esamtrade.vlada.ro", accessKey, secretKey, "minio-dev-tests");
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
    @Disabled("AWS SDK V1's S3ObjectInputStream does not support mark/reset operations required for Parquet reading")
    void testPutAndGetParquetObjectStream() throws IOException {
        tester.testPutAndGetParquetObjectStream();
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