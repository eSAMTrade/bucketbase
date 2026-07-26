package com.esamtrade.bucketbase;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeletedObject;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Error;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies {@link S3Bucket} behaviour that needs a controllable S3 client rather than a live
 * service: per-key delete-error reporting and client-ownership on close.
 */
class S3RemoveAndCloseTest {

    @Test
    void removeObjectsReturnsServerReportedErrors() throws Exception {
        FakeDeleteClient client = new FakeDeleteClient();
        // "denied.bin" fails; "ok.bin" and the missing key succeed silently.
        client.errorKey = "denied.bin";
        S3Bucket bucket = new S3Bucket(client, null, "bucket");

        List<DeleteError> errors = bucket.removeObjects(List.of(
                PurePosixPath.from("ok.bin"),
                PurePosixPath.from("denied.bin")));

        assertEquals(1, errors.size());
        DeleteError error = errors.get(0);
        assertEquals("denied.bin", error.name());
        assertEquals("AccessDenied", error.code());
        assertEquals("nope", error.message());
    }

    @Test
    void removeObjectsBatchesAtOneThousandKeys() throws Exception {
        FakeDeleteClient client = new FakeDeleteClient();
        S3Bucket bucket = new S3Bucket(client, null, "bucket");

        List<PurePosixPath> names = java.util.stream.IntStream.range(0, 2025)
                .mapToObj(i -> PurePosixPath.from("k" + i + ".bin"))
                .toList();
        assertTrue(bucket.removeObjects(names).isEmpty());

        // 2025 keys must be split into 1000 + 1000 + 25.
        assertEquals(List.of(1000, 1000, 25), client.batchSizes);
    }

    @Test
    void closeDoesNotShutDownAnInjectedClient() throws Exception {
        FakeDeleteClient client = new FakeDeleteClient();
        S3Bucket bucket = new S3Bucket(client, null, "bucket");

        bucket.close();

        assertFalse(client.closed, "a client passed in by the caller must not be closed by the bucket");
    }

    private static final class FakeDeleteClient implements S3Client {
        private final List<Integer> batchSizes = new java.util.ArrayList<>();
        private String errorKey;
        private boolean closed;

        @Override
        public DeleteObjectsResponse deleteObjects(DeleteObjectsRequest request) {
            List<ObjectIdentifier> objects = request.delete().objects();
            batchSizes.add(objects.size());
            DeleteObjectsResponse.Builder response = DeleteObjectsResponse.builder();
            List<DeletedObject> deleted = new java.util.ArrayList<>();
            List<S3Error> errors = new java.util.ArrayList<>();
            for (ObjectIdentifier object : objects) {
                if (object.key().equals(errorKey)) {
                    errors.add(S3Error.builder().key(object.key()).code("AccessDenied").message("nope").build());
                } else {
                    deleted.add(DeletedObject.builder().key(object.key()).build());
                }
            }
            return response.deleted(deleted).errors(errors).build();
        }

        @Override
        public String serviceName() {
            return "s3";
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
