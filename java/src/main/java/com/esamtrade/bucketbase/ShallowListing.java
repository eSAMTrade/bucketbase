package com.esamtrade.bucketbase;

import java.util.List;

/**
 * The result of a non-recursive listing: the objects directly under a prefix, plus the common
 * sub-prefixes (the equivalent of sub-directories).
 *
 * @param objects  object keys directly under the prefix
 * @param prefixes common child prefixes (each ending with {@code /})
 */
public record ShallowListing(List<PurePosixPath> objects, List<PurePosixPath> prefixes) {

    public ShallowListing {
        objects = List.copyOf(objects);
        prefixes = List.copyOf(prefixes);
    }
}
