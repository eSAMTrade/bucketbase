package com.esamtrade.bucketbase;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * S3BucketSDKv1 is a class that provides methods to interact with an S3 bucket.
 * It uses the old AWS SDK v1 for Java, kept for compatibility with SDK-v1 consumers.
 */
public class S3BucketSDKv1 implements IBucket {
    public static final int DEFAULT_READ_BUFFER_SIZE = 128 * 1024;

    private static final int DELETE_BATCH_SIZE = 1000;

    protected AmazonS3 s3Client;
    protected String bucketName;
    protected int readBufferSize;
    /** Whether this instance created the client (and must therefore shut it down). */
    private boolean ownsClient;

    public S3BucketSDKv1(String endpoint, String accessKey, String secretKey, String bucketName) {
        this(endpoint, accessKey, secretKey, bucketName, DEFAULT_READ_BUFFER_SIZE);
    }

    /** Default signing region, matching {@link S3Bucket}. S3-compatible services generally ignore it. */
    public static final String DEFAULT_REGION = "us-east-1";

    public S3BucketSDKv1(String endpoint, String accessKey, String secretKey, String bucketName, int readBufferSize) {
        this(endpoint, accessKey, secretKey, bucketName, readBufferSize, DEFAULT_REGION);
    }

    public S3BucketSDKv1(String endpoint, String accessKey, String secretKey, String bucketName, int readBufferSize, String region) {
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
        // The signing region must be a real region name: an empty string produces a signature
        // that S3-compatible servers reject with AuthorizationHeaderMalformed.
        AmazonS3 client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withPathStyleAccessEnabled(true)
                .build();
        initialize(client, bucketName, readBufferSize, true);
    }

    public S3BucketSDKv1(AmazonS3 s3Client, String bucketName) {
        this(s3Client, bucketName, DEFAULT_READ_BUFFER_SIZE);
    }

    public S3BucketSDKv1(AmazonS3 s3Client, String bucketName, int readBufferSize) {
        initialize(s3Client, bucketName, readBufferSize, false);
    }

    private void initialize(AmazonS3 s3Client, String bucketName, int readBufferSize, boolean ownsClient) {
        if (readBufferSize < 0) {
            throw new IllegalArgumentException("readBufferSize must be non-negative");
        }
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.readBufferSize = readBufferSize;
        this.ownsClient = ownsClient;
    }

    @Override
    public void putObject(PurePosixPath name, byte[] content) {
        String _name = S3Names.validateName(name);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(content.length);
        s3Client.putObject(bucketName, _name, new ByteArrayInputStream(content), metadata);
    }

    @Override
    public void putObjectStream(PurePosixPath name, InputStream stream) {
        String _name = S3Names.validateName(name);
        ObjectMetadata metadata = new ObjectMetadata();
        s3Client.putObject(bucketName, _name, stream, metadata);
    }

    @Override
    public byte[] getObject(PurePosixPath name) throws IOException {
        try (SeekableInputStream stream = getObjectStream(name)) {
            return stream.readAllBytes();
        }
    }

    @Override
    public SeekableInputStream getObjectStream(PurePosixPath name) throws IOException {
        String objectName = S3Names.validateName(name);
        try {
            ObjectMetadata metadata = s3Client.getObjectMetadata(bucketName, objectName);
            return new RangeSeekableInputStream(
                    metadata.getContentLength(),
                    readBufferSize,
                    (offset, dest, destOffset, length) -> readRange(objectName, metadata.getETag(), offset, dest, destOffset, length)
            );
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() != 404) {
                throw e;
            }
            throw new FileNotFoundException("Object " + name + " not found in S3 bucket " + bucketName);
        }
    }

    @Override
    public long getSize(PurePosixPath name) throws IOException {
        String objectName = S3Names.validateName(name);
        try {
            return s3Client.getObjectMetadata(bucketName, objectName).getContentLength();
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() != 404) {
                throw e;
            }
            throw new FileNotFoundException("Object " + name + " not found in S3 bucket " + bucketName);
        }
    }

    private void readRange(String objectName, String eTag, long offset, byte[] dest, int destOffset, int length) throws IOException {
        GetObjectRequest request = new GetObjectRequest(bucketName, objectName).withRange(offset, offset + length - 1);
        if (eTag != null) {
            request.withMatchingETagConstraint(eTag);
        }
        // SDK v1 returns null (rather than throwing) when a precondition such as
        // withMatchingETagConstraint is not met, so this must be checked before dereferencing.
        S3Object object = s3Client.getObject(request);
        if (object == null) {
            throw new IOException("Object " + objectName + " changed while the stream was open");
        }
        try (S3Object closeable = object; S3ObjectInputStream inputStream = closeable.getObjectContent()) {
            int read = inputStream.readNBytes(dest, destOffset, length);
            if (read != length) {
                throw new IOException("Expected " + length + " bytes at offset " + offset + " of " + objectName + ", but received " + read);
            }
        } catch (AmazonS3Exception e) {
            throw new IOException("Failed to read object " + objectName + " range at offset " + offset + " with length " + length, e);
        }
    }


    /**
     * Retrieves a list of object paths stored in the bucket that match the given prefix.
     * Note: this function can be slow for large buckets, as it retrieves all objects in the bucket.
     * If the response contains over 1000 objects, the S3 API paginates it, and the function retrieves all pages.
     *
     * @param prefix the path prefix used to filter objects
     * @return a list of PurePosixPath objects representing the matching objects
     */
    @Override
    public List<PurePosixPath> listObjects(PurePosixPath prefix) {
        String validated = S3Names.validatePrefix(prefix);
        List<PurePosixPath> result = new ArrayList<>();
        ObjectListing objectListing = s3Client.listObjects(bucketName, validated);
        while (true) {
            for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
                result.add(PurePosixPath.from(summary.getKey()));
            }
            if (!objectListing.isTruncated()) {
                break;
            }
            objectListing = s3Client.listNextBatchOfObjects(objectListing);
        }
        return result;
    }

    @Override
    public ShallowListing shallowListObjects(PurePosixPath prefix) {
        String validated = S3Names.validatePrefix(prefix);
        List<PurePosixPath> objects = new ArrayList<>();
        List<PurePosixPath> prefixes = new ArrayList<>();
        ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(validated).withDelimiter(PurePosixPath.SEP);
        ListObjectsV2Result result;
        do {
            result = s3Client.listObjectsV2(request);
            for (S3ObjectSummary summary : result.getObjectSummaries()) {
                objects.add(PurePosixPath.from(summary.getKey()));
            }
            prefixes.addAll(result.getCommonPrefixes().stream().map(PurePosixPath::from).toList());
            request.setContinuationToken(result.getNextContinuationToken());
        } while (result.isTruncated());

        return new ShallowListing(objects, prefixes);
    }

    @Override
    public boolean exists(PurePosixPath name) {
        String _name = S3Names.validateName(name);
        return s3Client.doesObjectExist(bucketName, _name);
    }

    /**
     * Deletes objects in batches of 1000 (S3's per-request limit). Deleting a missing key is not
     * an error; keys the server reports as failed are returned as {@link DeleteError}s. A partial
     * failure raises {@link MultiObjectDeleteException} in SDK v1, whose per-key errors are
     * collected rather than propagated.
     */
    @Override
    public List<DeleteError> removeObjects(List<PurePosixPath> names) {
        List<String> validated = names.stream().map(S3Names::validateName).toList();
        List<DeleteError> errors = new ArrayList<>();
        for (int i = 0; i < validated.size(); i += DELETE_BATCH_SIZE) {
            List<String> batch = validated.subList(i, Math.min(i + DELETE_BATCH_SIZE, validated.size()));
            List<DeleteObjectsRequest.KeyVersion> keys = batch.stream()
                    .map(DeleteObjectsRequest.KeyVersion::new)
                    .toList();
            try {
                s3Client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keys).withQuiet(true));
            } catch (MultiObjectDeleteException e) {
                for (MultiObjectDeleteException.DeleteError error : e.getErrors()) {
                    errors.add(new DeleteError(error.getCode(), error.getMessage(), error.getKey()));
                }
            }
        }
        return errors;
    }

    @Override
    public void close() {
        if (ownsClient) {
            s3Client.shutdown();
        }
    }
}
