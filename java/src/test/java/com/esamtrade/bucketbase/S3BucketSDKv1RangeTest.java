package com.esamtrade.bucketbase;

import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class S3BucketSDKv1RangeTest {
    @Test
    void publicStreamUsesMetadataOnceAndBoundedConditionalGets() throws IOException {
        byte[] content = "0123456789abcdefghijklmnopqrstuvwxyz".getBytes();
        FakeAmazonS3 client = new FakeAmazonS3(content);
        S3BucketSDKv1 bucket = new S3BucketSDKv1(client, "test-bucket", 8);

        try (SeekableInputStream stream = bucket.getObjectStream(PurePosixPath.from("object.bin"))) {
            stream.seek(5);
            assertArrayEquals("56".getBytes(), stream.readNBytes(2));
            stream.seek(8);
            assertArrayEquals("89".getBytes(), stream.readNBytes(2));
        }

        assertEquals(1, client.metadataCalls);
        assertEquals(1, client.getRequests.size());
        GetObjectRequest request = client.getRequests.get(0);
        assertArrayEquals(new long[]{5, 12}, request.getRange());
        assertEquals(List.of("test-etag"), request.getMatchingETagConstraints());
    }

    @Test
    void fullReadIsStillOneBoundedGetAndBufferSizeIsValidated() throws IOException {
        byte[] content = "0123456789abcdefghijklmnopqrstuvwxyz".getBytes();
        FakeAmazonS3 client = new FakeAmazonS3(content);
        S3BucketSDKv1 bucket = new S3BucketSDKv1(client, "test-bucket", 8);

        assertArrayEquals(content, bucket.getObject(PurePosixPath.from("object.bin")));

        assertEquals(1, client.getRequests.size());
        assertArrayEquals(new long[]{0, 35}, client.getRequests.get(0).getRange());
        assertThrows(IllegalArgumentException.class, () -> new S3BucketSDKv1(client, "test-bucket", -1));
    }

    private static final class FakeAmazonS3 extends AbstractAmazonS3 {
        private final byte[] content;
        private final List<GetObjectRequest> getRequests = new ArrayList<>();
        private int metadataCalls;

        private FakeAmazonS3(byte[] content) {
            this.content = content;
        }

        @Override
        public ObjectMetadata getObjectMetadata(String bucketName, String key) {
            metadataCalls++;
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(content.length);
            metadata.setHeader("ETag", "test-etag");
            return metadata;
        }

        @Override
        public S3Object getObject(GetObjectRequest request) {
            getRequests.add(request);
            int start = Math.toIntExact(request.getRange()[0]);
            int endExclusive = Math.toIntExact(request.getRange()[1] + 1);
            S3Object object = new S3Object();
            object.setObjectContent(new ByteArrayInputStream(Arrays.copyOfRange(content, start, endExclusive)));
            return object;
        }
    }
}
