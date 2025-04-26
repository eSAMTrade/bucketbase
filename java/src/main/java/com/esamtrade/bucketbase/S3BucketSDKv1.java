package com.esamtrade.bucketbase;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * S3BucketSDKv1 is a class that provides methods to interact with an S3 bucket.
 * It extends the BaseBucket class and uses the old AWS SDK v1 for Java
 */
public class S3BucketSDKv1 extends BaseBucket {

    protected AmazonS3 s3Client;
    protected String bucketName;

    public S3BucketSDKv1(String endpoint, String accessKey, String secretKey, String bucketName) {
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
        this.s3Client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, ""))
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withPathStyleAccessEnabled(true)
                .build();
        this.bucketName = bucketName;
    }

    @Override
    public void putObject(PurePosixPath name, byte[] content) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(content.length);
        s3Client.putObject(bucketName, name.toString(), new ByteArrayInputStream(content), metadata);
    }

    @Override
    public void putObjectStream(PurePosixPath name, InputStream stream) {
        ObjectMetadata metadata = new ObjectMetadata();
        s3Client.putObject(bucketName, name.toString(), stream, metadata);
    }

    @Override
    public byte[] getObject(PurePosixPath name) throws IOException {
        S3Object s3Object;
        try {
            s3Object = s3Client.getObject(bucketName, name.toString());
        } catch (AmazonS3Exception e) {
            throw new FileNotFoundException("Object " + name + " not found in S3 bucket " + bucketName);
        }
        S3ObjectInputStream inputStream = s3Object.getObjectContent();
        return IOUtils.toByteArray(inputStream);
    }

    @Override
    public ObjectStream getObjectStream(PurePosixPath name) throws IOException {
        S3Object s3Object;
        try {
            s3Object = s3Client.getObject(bucketName, name.toString());
        } catch (AmazonS3Exception e) {
            throw new FileNotFoundException("Object " + name + " not found in S3 bucket " + bucketName);
        }
        S3ObjectInputStream inputStream = s3Object.getObjectContent();
        return new ObjectStream(inputStream, name.toString());
    }

    @Override
    public long getSize(PurePosixPath name) throws IOException {
        try {
            return s3Client.getObjectMetadata(bucketName, name.toString()).getContentLength();
        }
        catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404)
                throw new FileNotFoundException("Object " + name + " not found in S3 bucket " + bucketName);
            throw new IOException("Failed to get object metadata: " + e.getMessage(), e);
        } catch (SdkClientException e) {
            throw new IOException("Failed to get object metadata: " + e.getMessage(), e);
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
        splitPrefix(prefix); // validate prefix
        List<PurePosixPath> result = new ArrayList<>();
        ObjectListing objectListing = s3Client.listObjects(bucketName, prefix.toString());
        while (true) {
            for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
                result.add(new PurePosixPath(summary.getKey()));
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
        splitPrefix(prefix); // validate prefix
        List<PurePosixPath> objects = new ArrayList<>();
        List<PurePosixPath> prefixes = new ArrayList<>();
        ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(prefix.toString()).withDelimiter(SEP);
        ListObjectsV2Result result;
        do {
            result = s3Client.listObjectsV2(request);
            for (S3ObjectSummary summary : result.getObjectSummaries()) {
                objects.add(new PurePosixPath(summary.getKey()));
            }
            prefixes.addAll(result.getCommonPrefixes().stream().map(PurePosixPath::new).toList());
            request.setContinuationToken(result.getNextContinuationToken());
        } while (result.isTruncated());

        return new ShallowListing(objects, prefixes);
    }

    @Override
    public boolean exists(PurePosixPath name) {
        String _name = validateName(name);
        return s3Client.doesObjectExist(bucketName, _name);
    }

    @Override
    public List<DeleteError> removeObjects(List<PurePosixPath> names) {
        Set<String> namesSet = names.stream().map(BaseBucket::validateName).collect(Collectors.toSet());
        List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
        for (PurePosixPath name : names) {
            keys.add(new DeleteObjectsRequest.KeyVersion(name.toString()));
        }
        List<DeleteError> errors = new ArrayList<>();
        if (!keys.isEmpty()) {
            DeleteObjectsRequest request = new DeleteObjectsRequest(bucketName).withKeys(keys);
            DeleteObjectsResult result = s3Client.deleteObjects(request);

            for (DeleteObjectsResult.DeletedObject deleted : result.getDeletedObjects()) {
                if (!namesSet.contains(deleted.getKey())) {
                    errors.add(new DeleteError("Object not found: " + deleted.getKey()));
                }
            }
        }
        return errors;
    }
}
