package com.esamtrade.bucketbase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.IOException;

/**
 * Functional tests against a live S3-compatible service. Endpoint and credentials come from
 * {@link TestConfig}, which defaults to the public MinIO playground so this runs without any
 * private secrets. Set {@code BUCKETBASE_SKIP_FUNCTIONAL_TESTS=1} to skip in offline builds.
 */
@EnabledIf("com.esamtrade.bucketbase.TestConfig#functionalTestsEnabled")
class S3BucketTest {

    private S3Bucket bucket;
    private IBucketTester tester;

    @BeforeEach
    public void setUp() {
        bucket = new S3Bucket(
                TestConfig.MINIO_ENDPOINT,
                TestConfig.MINIO_ACCESS_KEY,
                TestConfig.MINIO_SECRET_KEY,
                TestConfig.MINIO_BUCKET);
        TestConfig.ensureBucketExists(bucket);
        tester = new IBucketTester(bucket);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (tester != null) {
            tester.cleanup();
        }
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
