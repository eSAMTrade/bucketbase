package com.esamtrade.bucketbase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.IOException;

/**
 * Functional tests for the AWS SDK v1 backend. See {@link S3BucketTest} for how the endpoint
 * and credentials are configured.
 */
@EnabledIf("com.esamtrade.bucketbase.TestConfig#functionalTestsEnabled")
class S3BucketSDKv1Test {
    private S3BucketSDKv1 bucket;
    private IBucketTester tester;

    @BeforeEach
    public void setUp() {
        bucket = new S3BucketSDKv1(
                TestConfig.MINIO_ENDPOINT,
                TestConfig.MINIO_ACCESS_KEY,
                TestConfig.MINIO_SECRET_KEY,
                TestConfig.MINIO_BUCKET);
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
    void getObjectStreamIsSeekable() throws IOException {
        tester.testGetObjectStreamIsSeekable();
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
