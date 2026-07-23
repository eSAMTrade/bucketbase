package com.esamtrade.bucketbase;

/**
 * A single failed deletion returned by {@link IBucket#removeObjects(java.util.List)}.
 *
 * <p>This is a value type (not an exception): {@code removeObjects} is a bulk operation that
 * reports per-key failures without aborting, so callers inspect the returned list. Mirrors the
 * Python library's {@code DeleteError}.</p>
 *
 * @param code    the storage-provider error code (e.g. {@code AccessDenied})
 * @param message the human-readable error message
 * @param name    the object key that failed to delete
 */
public record DeleteError(String code, String message, String name) {
}
