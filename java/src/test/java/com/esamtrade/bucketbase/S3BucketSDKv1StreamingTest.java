package com.esamtrade.bucketbase;

import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for streamed uploads through the AWS SDK v1 backend.
 *
 * <p>SDK v1's {@code putObject(bucket, key, stream, metadata)} buffers the <em>entire</em> stream
 * into memory when the metadata carries no content length, so it can compute one. That defeats the
 * whole point of {@link IBucket#openWrite(PurePosixPath)}, which exists to stream payloads too
 * large to hold in memory. The backend must therefore upload in bounded parts, exactly as
 * {@link S3Bucket} does.</p>
 */
class S3BucketSDKv1StreamingTest {

    private static final int PART_SIZE = 5 * 1024 * 1024;
    private static final PurePosixPath OBJECT = PurePosixPath.from("dir/streamed.bin");

    @Test
    void streamedUploadUsesMultipartAndNeverBuffersTheWholeStream() throws IOException {
        // Larger than one part, so a correct implementation must issue several uploadPart calls.
        byte[] payload = payload(12 * 1024 * 1024);
        CountingInputStream source = new CountingInputStream(payload);
        FakeMultipartS3 client = new FakeMultipartS3();
        client.source = source;
        S3BucketSDKv1 bucket = new S3BucketSDKv1(client, "test-bucket");

        bucket.putObjectStream(OBJECT, source);

        assertEquals(0, client.singleShotPutObjectCalls,
                "a streamed upload must not go through the buffering single-shot putObject");
        assertTrue(client.uploadedParts.size() >= 2,
                "expected a multipart upload, got " + client.uploadedParts.size() + " part(s)");
        assertEquals(1, client.completedUploads.size(), "the multipart upload must be completed");

        // The heart of the regression: the first part must be sent before the source is drained.
        assertTrue(client.bytesReadWhenFirstPartUploaded >= 0, "no part was uploaded at all");
        assertTrue(client.bytesReadWhenFirstPartUploaded < payload.length,
                "the whole stream was read (" + client.bytesReadWhenFirstPartUploaded + " of " + payload.length
                        + " bytes) before the first part was uploaded, i.e. it was buffered in memory");

        assertArrayEquals(payload, client.assembled(), "the reassembled object must match the source bytes");
    }

    @Test
    void everyPartExceptTheLastIsAtLeastTheMinimumPartSize() throws IOException {
        // S3 rejects a non-final part smaller than 5 MiB with EntityTooSmall, and a queue-backed
        // sink hands over whatever chunk sizes the caller wrote, so parts must be coalesced.
        byte[] payload = payload(11 * 1024 * 1024);
        FakeMultipartS3 client = new FakeMultipartS3();
        S3BucketSDKv1 bucket = new S3BucketSDKv1(client, "test-bucket");

        bucket.putObjectStream(OBJECT, new ChunkedInputStream(payload, 8 * 1024));

        List<byte[]> parts = client.uploadedParts;
        for (int i = 0; i < parts.size() - 1; i++) {
            assertTrue(parts.get(i).length >= PART_SIZE,
                    "part " + (i + 1) + " is only " + parts.get(i).length + " bytes; S3 requires >= " + PART_SIZE);
        }
        assertArrayEquals(payload, client.assembled());
    }

    @Test
    void emptyStreamStillStoresAnObject() throws IOException {
        FakeMultipartS3 client = new FakeMultipartS3();
        S3BucketSDKv1 bucket = new S3BucketSDKv1(client, "test-bucket");

        bucket.putObjectStream(OBJECT, new ByteArrayInputStream(new byte[0]));

        assertEquals(1, client.completedUploads.size(), "an empty body must still produce a stored object");
        assertEquals(0, client.assembled().length);
    }

    @Test
    void aFailureMidUploadAbortsAndDoesNotComplete() {
        FakeMultipartS3 client = new FakeMultipartS3();
        client.failOnPart = 2;
        S3BucketSDKv1 bucket = new S3BucketSDKv1(client, "test-bucket");

        assertThrows(IOException.class,
                () -> bucket.putObjectStream(OBJECT, new ByteArrayInputStream(payload(12 * 1024 * 1024))));

        assertEquals(0, client.completedUploads.size(), "a failed upload must not be completed");
        assertEquals(1, client.abortedUploads.size(), "a failed upload must be aborted so no partial object lingers");
    }

    private static byte[] payload(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 251);
        }
        return data;
    }

    /** Records how many bytes the uploader has pulled from the source so far. */
    private static final class CountingInputStream extends InputStream {
        private final byte[] data;
        private int pos;

        CountingInputStream(byte[] data) {
            this.data = data;
        }

        int bytesRead() {
            return pos;
        }

        @Override
        public int read() {
            return pos < data.length ? data[pos++] & 0xff : -1;
        }

        @Override
        public int read(byte[] dest, int off, int len) {
            if (pos >= data.length) {
                return -1;
            }
            int n = Math.min(len, data.length - pos);
            System.arraycopy(data, pos, dest, off, n);
            pos += n;
            return n;
        }
    }

    /** Returns at most {@code chunk} bytes per read, like a queue-backed sink does. */
    private static final class ChunkedInputStream extends InputStream {
        private final byte[] data;
        private final int chunk;
        private int pos;

        ChunkedInputStream(byte[] data, int chunk) {
            this.data = data;
            this.chunk = chunk;
        }

        @Override
        public int read() {
            return pos < data.length ? data[pos++] & 0xff : -1;
        }

        @Override
        public int read(byte[] dest, int off, int len) {
            if (pos >= data.length) {
                return -1;
            }
            int n = Math.min(Math.min(len, chunk), data.length - pos);
            System.arraycopy(data, pos, dest, off, n);
            pos += n;
            return n;
        }
    }

    private static final class FakeMultipartS3 extends AbstractAmazonS3 {
        private final List<byte[]> uploadedParts = new ArrayList<>();
        private final List<String> completedUploads = new ArrayList<>();
        private final List<String> abortedUploads = new ArrayList<>();
        private int singleShotPutObjectCalls;
        private int bytesReadWhenFirstPartUploaded = -1;
        private int failOnPart = -1;
        private CountingInputStream source;

        byte[] assembled() {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            uploadedParts.forEach(out::writeBytes);
            return out.toByteArray();
        }

        @Override
        public PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata) {
            singleShotPutObjectCalls++;
            return new PutObjectResult();
        }

        @Override
        public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request) {
            InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
            result.setUploadId("upload-1");
            return result;
        }

        @Override
        public UploadPartResult uploadPart(UploadPartRequest request) {
            if (uploadedParts.size() + 1 == failOnPart) {
                throw new IllegalStateException("simulated part upload failure");
            }
            byte[] part;
            try (InputStream in = request.getInputStream()) {
                part = in.readNBytes(Math.toIntExact(request.getPartSize()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            uploadedParts.add(part);
            if (bytesReadWhenFirstPartUploaded < 0 && source != null) {
                bytesReadWhenFirstPartUploaded = source.bytesRead();
            }
            UploadPartResult result = new UploadPartResult();
            result.setPartNumber(request.getPartNumber());
            result.setETag("etag-" + request.getPartNumber());
            return result;
        }

        @Override
        public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) {
            completedUploads.add(request.getUploadId());
            return new CompleteMultipartUploadResult();
        }

        @Override
        public void abortMultipartUpload(AbortMultipartUploadRequest request) {
            abortedUploads.add(request.getUploadId());
        }
    }
}
