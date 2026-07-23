package com.esamtrade.bucketbase;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class S3BucketRangeTest {
    @Test
    void publicStreamUsesHeadOnceAndBoundedConditionalGets() throws IOException {
        byte[] content = "0123456789abcdefghijklmnopqrstuvwxyz".getBytes();
        FakeS3Client client = new FakeS3Client(content);
        S3Bucket bucket = new S3Bucket(client, null, "test-bucket", 8);

        try (SeekableInputStream stream = bucket.getObjectStream(PurePosixPath.from("object.bin"))) {
            stream.seek(5);
            assertArrayEquals("56".getBytes(), stream.readNBytes(2));
            stream.seek(8);
            assertArrayEquals("89".getBytes(), stream.readNBytes(2));
        }

        assertEquals(1, client.headRequests.size());
        assertEquals(1, client.getRequests.size());
        GetObjectRequest request = client.getRequests.get(0);
        assertEquals("bytes=5-12", request.range());
        assertEquals("test-etag", request.ifMatch());
        assertNotNull(request.range());
    }

    @Test
    void fullReadIsStillOneBoundedGetAndBufferSizeIsValidated() throws IOException {
        byte[] content = "0123456789abcdefghijklmnopqrstuvwxyz".getBytes();
        FakeS3Client client = new FakeS3Client(content);
        S3Bucket bucket = new S3Bucket(client, null, "test-bucket", 8);

        assertArrayEquals(content, bucket.getObject(PurePosixPath.from("object.bin")));

        assertEquals(1, client.getRequests.size());
        assertEquals("bytes=0-35", client.getRequests.get(0).range());
        assertThrows(IllegalArgumentException.class, () -> new S3Bucket(client, null, "test-bucket", -1));
    }

    private static final class FakeS3Client implements S3Client {
        private final byte[] content;
        private final List<HeadObjectRequest> headRequests = new ArrayList<>();
        private final List<GetObjectRequest> getRequests = new ArrayList<>();

        private FakeS3Client(byte[] content) {
            this.content = content;
        }

        @Override
        public HeadObjectResponse headObject(HeadObjectRequest request) {
            headRequests.add(request);
            return HeadObjectResponse.builder().contentLength((long) content.length).eTag("test-etag").build();
        }

        @Override
        public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest request) {
            getRequests.add(request);
            String[] bounds = request.range().substring("bytes=".length()).split("-");
            int start = Integer.parseInt(bounds[0]);
            int endInclusive = Integer.parseInt(bounds[1]);
            byte[] response = Arrays.copyOfRange(content, start, endInclusive + 1);
            return new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(response));
        }

        @Override
        public String serviceName() {
            return "s3";
        }

        @Override
        public void close() {
        }
    }
}
