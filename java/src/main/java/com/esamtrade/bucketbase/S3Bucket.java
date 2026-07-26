package com.esamtrade.bucketbase;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class S3Bucket implements IBucket {
    public static final int DEFAULT_READ_BUFFER_SIZE = 128 * 1024;
    private static final int DELETE_BATCH_SIZE = 1000;

    protected S3Client s3Client;
    protected S3AsyncClient s3AsyncClient;
    protected String bucketName;
    protected int readBufferSize;
    /** Whether this instance created the clients (and must therefore close them). */
    private final boolean ownsClients;

    public S3Bucket(S3Client s3Client, S3AsyncClient s3AsyncClient, String bucketName) {
        this(s3Client, s3AsyncClient, bucketName, DEFAULT_READ_BUFFER_SIZE);
    }

    public S3Bucket(S3Client s3Client, S3AsyncClient s3AsyncClient, String bucketName, int readBufferSize) {
        this(s3Client, s3AsyncClient, bucketName, readBufferSize, false);
    }

    private S3Bucket(S3Client s3Client, S3AsyncClient s3AsyncClient, String bucketName, int readBufferSize, boolean ownsClients) {
        if (readBufferSize < 0) {
            throw new IllegalArgumentException("readBufferSize must be non-negative");
        }
        this.s3Client = s3Client;
        this.s3AsyncClient = s3AsyncClient;
        this.bucketName = bucketName;
        this.readBufferSize = readBufferSize;
        this.ownsClients = ownsClients;
    }

    public S3Bucket(String endpoint, String accessKey, String secretKey, String bucketName) {
        this(endpoint, accessKey, secretKey, bucketName, DEFAULT_READ_BUFFER_SIZE);
    }

    public S3Bucket(String endpoint, String accessKey, String secretKey, String bucketName, int readBufferSize) {
        this(createS3Client(endpoint, accessKey, secretKey), createS3AsyncClient(endpoint, accessKey, secretKey), bucketName, readBufferSize, true);
    }

    private static S3Client createS3Client(String endpoint, String accessKey, String secretKey) {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKey, secretKey);
        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .region(Region.US_EAST_1) // Default region, can be parameterized if needed
                .forcePathStyle(true)
                .build();
    }

    private static S3AsyncClient createS3AsyncClient(String endpoint, String accessKey, String secretKey) {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKey, secretKey);
        return S3AsyncClient.builder()
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .region(Region.US_EAST_1) // Default region, can be parameterized if needed
                .forcePathStyle(true)
                .build();
    }

    @Override
    public void putObject(PurePosixPath name, byte[] content) {
        String _name = S3Names.validateName(name);
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(_name)
                .build();
        s3Client.putObject(request, RequestBody.fromBytes(content));
    }

    @Override
    public void putObjectStream(PurePosixPath name, InputStream stream) throws IOException {
        String _name = S3Names.validateName(name);
        uploadLargeStream(_name, stream);
    }

    private void uploadLargeStream(String key, InputStream inputStream) throws IOException {
        // S3 requires every part except the last to be at least 5 MiB.
        final int partSize = 5 * 1024 * 1024;
        List<CompletedPart> completedParts = new ArrayList<>();
        int partNumber = 1;

        // 1. Initiate the multipart upload
        CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        CreateMultipartUploadResponse response = s3AsyncClient.createMultipartUpload(createMultipartUploadRequest).join();
        String uploadId = response.uploadId();

        try {
            // 2. Read the input stream and upload each part.
            //    readNBytes fills the buffer across however many short reads the source needs -
            //    a plain read(buffer) may legally return far less than partSize without being at
            //    EOF, which would produce undersized parts and an EntityTooSmall failure.
            while (true) {
                byte[] part = inputStream.readNBytes(partSize);
                boolean lastPart = part.length < partSize;
                // Skip a trailing empty read, but keep a single empty part for an empty object:
                // completeMultipartUpload rejects an upload with no parts at all.
                if (part.length > 0 || completedParts.isEmpty()) {
                    UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .uploadId(uploadId)
                            .partNumber(partNumber)
                            .contentLength((long) part.length)
                            .build();
                    AsyncRequestBody requestBody = AsyncRequestBody.fromBytes(part);
                    CompletableFuture<UploadPartResponse> uploadPartResponse = s3AsyncClient.uploadPart(uploadPartRequest, requestBody);
                    completedParts.add(CompletedPart.builder()
                            .partNumber(partNumber)
                            .eTag(uploadPartResponse.join().eTag())
                            .build());
                    partNumber++;
                }
                if (lastPart) {
                    break;
                }
            }

            // 3. Complete the multipart upload
            CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                    .build();
            s3AsyncClient.completeMultipartUpload(completeMultipartUploadRequest).join();
        } catch (Exception e) {
            abortQuietly(key, uploadId, e);
            throw new IOException("Failed to upload object: " + key, e);
        }
    }

    /**
     * Aborts a multipart upload without letting the abort failure hide the original cause.
     */
    private void abortQuietly(String key, String uploadId, Exception primary) {
        try {
            s3AsyncClient.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .build()).join();
        } catch (Exception abortFailure) {
            primary.addSuppressed(abortFailure);
        }
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
        HeadObjectResponse metadata = headObject(objectName);
        return new RangeSeekableInputStream(
                contentLengthOf(metadata, objectName),
                readBufferSize,
                (offset, dest, destOffset, length) -> readRange(objectName, metadata.eTag(), offset, dest, destOffset, length)
        );
    }

    @Override
    public long getSize(PurePosixPath name) throws IOException {
        String objectName = S3Names.validateName(name);
        return contentLengthOf(headObject(objectName), objectName);
    }

    private HeadObjectResponse headObject(String objectName) throws IOException {
        try {
            return s3Client.headObject(HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectName)
                    .build());
        } catch (S3Exception e) {
            // HEAD has no response body, so many S3-compatible servers (MinIO included) return a
            // bare 404 that does not deserialize into NoSuchKeyException. Match on the status.
            if (e.statusCode() == 404) {
                throw new FileNotFoundException("Object " + objectName + " not found in S3 bucket " + bucketName);
            }
            throw e;
        }
    }

    private static long contentLengthOf(HeadObjectResponse metadata, String objectName) throws IOException {
        Long contentLength = metadata.contentLength();
        if (contentLength == null) {
            throw new IOException("S3 did not report a content length for object " + objectName);
        }
        return contentLength;
    }

    private void readRange(String objectName, String eTag, long offset, byte[] dest, int destOffset, int length) throws IOException {
        GetObjectRequest.Builder request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectName)
                .range("bytes=" + offset + "-" + (offset + length - 1));
        if (eTag != null) {
            request.ifMatch(eTag);
        }
        try (ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request.build())) {
            // Read straight into the caller's buffer - no intermediate copy for large reads.
            int read = response.readNBytes(dest, destOffset, length);
            if (read != length) {
                throw new IOException("Expected " + length + " bytes at offset " + offset + " of " + objectName + ", but received " + read);
            }
        } catch (S3Exception e) {
            throw new IOException("Failed to read object " + objectName + " range at offset " + offset + " with length " + length, e);
        }
    }


    @Override
    /**
     * Lists all objects in the S3 bucket with the given prefix.
     *
     * <p>This method performs pagination to retrieve all objects, which might be slow for large datasets.</p>
     *
     * @param prefix The prefix to filter objects by.
     * @return A list of paths to the objects in the bucket.
     */
    public List<PurePosixPath> listObjects(PurePosixPrefix prefix) {
        String validated = S3Names.validatePrefix(prefix);
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(validated)
                .build();
        // The paginator transparently follows continuation tokens across all pages.
        return s3Client.listObjectsV2Paginator(request).contents().stream()
                .map(S3Object::key)
                .map(PurePosixPath::from)
                .collect(java.util.stream.Collectors.toList());
    }

    @Override
    public ShallowListing shallowListObjects(PurePosixPrefix prefix) {
        String validated = S3Names.validatePrefix(prefix);
        List<PurePosixPath> objects = new ArrayList<>();
        List<PurePosixPrefix> prefixes = new ArrayList<>();

        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(validated)
                .delimiter(PurePosixPrefix.SEP)
                .build();

        s3Client.listObjectsV2Paginator(request).forEach(response -> {
            for (S3Object object : response.contents()) {
                objects.add(PurePosixPath.from(object.key()));
            }
            for (CommonPrefix commonPrefix : response.commonPrefixes()) {
                prefixes.add(PurePosixPrefix.from(commonPrefix.prefix()));
            }
        });

        return new ShallowListing(objects, prefixes);
    }

    @Override
    public boolean exists(PurePosixPath name) {
        String _name = S3Names.validateName(name);
        try {
            s3Client.headObject(HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(_name)
                    .build());
            return true;
        } catch (S3Exception e) {
            // A HEAD 404 does not always deserialize into NoSuchKeyException on S3-compatible
            // servers, so treat any 404 as "does not exist".
            if (e.statusCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    /**
     * Deletes multiple objects in a single request per 1000 keys. Deleting a key that does not
     * exist is not an error (consistent with S3). Any keys the server reports as failed - for
     * example due to permissions - are returned as {@link DeleteError}s rather than thrown.
     */
    @Override
    public List<DeleteError> removeObjects(List<PurePosixPath> names) {
        List<String> validatedNames = names.stream().map(S3Names::validateName).toList();
        List<DeleteError> allErrors = new ArrayList<>();
        for (int i = 0; i < validatedNames.size(); i += DELETE_BATCH_SIZE) {
            List<String> batch = validatedNames.subList(i, Math.min(i + DELETE_BATCH_SIZE, validatedNames.size()));
            allErrors.addAll(removeBatch(batch));
        }
        return allErrors;
    }

    private List<DeleteError> removeBatch(List<String> names) {
        if (names.isEmpty()) {
            return List.of();
        }
        List<ObjectIdentifier> keys = names.stream()
                .map(name -> ObjectIdentifier.builder().key(name).build())
                .toList();
        DeleteObjectsResponse response = s3Client.deleteObjects(DeleteObjectsRequest.builder()
                .bucket(bucketName)
                .delete(Delete.builder().objects(keys).build())
                .build());
        // response.errors() carries the per-key failures; response.deleted() are the successes.
        return response.errors().stream()
                .map(error -> new DeleteError(error.code(), error.message(), error.key()))
                .collect(java.util.stream.Collectors.toList());
    }

    @Override
    public void close() {
        if (ownsClients) {
            s3Client.close();
            if (s3AsyncClient != null) {
                s3AsyncClient.close();
            }
        }
    }
}
