package com.esamtrade.bucketbase;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Test configuration, mirroring {@code python/tests/base_config.py}.
 *
 * <p>Defaults point at the public MinIO playground with its well-known demo credentials, so the
 * functional tests run against a real S3-compatible service without any private secrets. Every
 * value can be overridden with an environment variable to target a private instance.</p>
 */
public final class TestConfig {

    private TestConfig() {
    }

    /** Endpoint URL of the S3-compatible service under test. */
    public static final String MINIO_ENDPOINT =
            envOrDefault("MINIO_ENDPOINT", "https://play.min.io");

    public static final String MINIO_ACCESS_KEY =
            envOrDefault("MINIO_ACCESS_KEY", "Q3AM3UQ867SPQQA43P2F");

    public static final String MINIO_SECRET_KEY =
            envOrDefault("MINIO_SECRET_KEY", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG");

    public static final String MINIO_BUCKET =
            envOrDefault("MINIO_DEV_TESTS_BUCKET", "bucketbase-test");

    /**
     * Functional tests are opt-out rather than opt-in: they run by default against the public
     * playground, and can be disabled with {@code BUCKETBASE_SKIP_FUNCTIONAL_TESTS=1} for
     * offline or air-gapped builds.
     */
    public static boolean functionalTestsEnabled() {
        String skip = System.getenv("BUCKETBASE_SKIP_FUNCTIONAL_TESTS");
        return skip == null || skip.isBlank() || skip.equals("0") || skip.equalsIgnoreCase("false");
    }

    /**
     * Ensures the configured bucket exists before an SDK v2 functional test uses it.
     *
     * <p>Creation is deliberately test-fixture behavior; constructing a production
     * {@link S3Bucket} must not mutate S3 infrastructure.</p>
     */
    static void ensureBucketExists(S3Bucket bucket) {
        HeadBucketRequest headRequest = HeadBucketRequest.builder()
                .bucket(bucket.bucketName)
                .build();
        try {
            bucket.s3Client.headBucket(headRequest);
            return;
        } catch (S3Exception exception) {
            if (exception.statusCode() != 404) {
                throw exception;
            }
        }

        try {
            bucket.s3Client.createBucket(CreateBucketRequest.builder()
                    .bucket(bucket.bucketName)
                    .build());
        } catch (S3Exception exception) {
            // Another concurrently running test process may have created the bucket.
            if (exception.statusCode() != 409) {
                throw exception;
            }
        }
        bucket.s3Client.headBucket(headRequest);
    }

    /** Ensures the configured bucket exists before an SDK v1 functional test uses it. */
    static void ensureBucketExists(S3BucketSDKv1 bucket) {
        if (!bucket.s3Client.doesBucketExistV2(bucket.bucketName)) {
            try {
                bucket.s3Client.createBucket(bucket.bucketName);
            } catch (AmazonS3Exception exception) {
                // Another concurrently running test process may have created the bucket.
                if (exception.getStatusCode() != 409) {
                    throw exception;
                }
            }
        }
        if (!bucket.s3Client.doesBucketExistV2(bucket.bucketName)) {
            throw new IllegalStateException("S3 test bucket is not accessible: " + bucket.bucketName);
        }
    }

    private static String envOrDefault(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
