package com.esamtrade.bucketbase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * An in-memory {@link IBucket}, intended for tests and local development.
 *
 * <p>Stored bytes are copied on the way in and out, so a caller mutating an array it passed to
 * {@link #putObject} — or one returned by {@link #getObject} — cannot corrupt stored objects.
 * This matches the value semantics of the real backends. Backed by a {@link ConcurrentHashMap},
 * so concurrent access is safe.</p>
 */
public class MemoryBucket implements IBucket {

    private final ConcurrentHashMap<String, byte[]> objects = new ConcurrentHashMap<>();

    @Override
    public void putObject(PurePosixPath name, byte[] content) {
        String _name = S3Names.validateName(name);
        objects.put(_name, content.clone());
    }

    @Override
    public void putObjectStream(PurePosixPath name, InputStream stream) throws IOException {
        String _name = S3Names.validateName(name);
        objects.put(_name, stream.readAllBytes());
    }

    @Override
    public byte[] getObject(PurePosixPath name) throws FileNotFoundException {
        return readObject(name).clone();
    }

    @Override
    public SeekableInputStream getObjectStream(PurePosixPath name) throws FileNotFoundException {
        byte[] content = readObject(name);
        return new RangeSeekableInputStream(content.length, 0,
                (offset, dest, destOffset, length) -> System.arraycopy(content, (int) offset, dest, destOffset, length));
    }

    @Override
    public long getSize(PurePosixPath name) throws FileNotFoundException {
        return readObject(name).length;
    }

    private byte[] readObject(PurePosixPath name) throws FileNotFoundException {
        String _name = S3Names.validateName(name);
        byte[] content = objects.get(_name);
        if (content == null) {
            throw new FileNotFoundException("Object " + _name + " not found in MemoryBucket");
        }
        return content;
    }

    @Override
    public List<PurePosixPath> listObjects(PurePosixPath prefix) {
        String strPrefix = S3Names.validatePrefix(prefix);
        return objects.keySet().stream()
                .filter(key -> key.startsWith(strPrefix))
                .map(PurePosixPath::from)
                .collect(Collectors.toList());
    }

    @Override
    public ShallowListing shallowListObjects(PurePosixPath prefix) {
        String strPrefix = S3Names.validatePrefix(prefix);
        int prefLen = strPrefix.length();
        List<PurePosixPath> objectsList = new ArrayList<>();
        Set<PurePosixPath> prefixesSet = new HashSet<>();
        for (String key : objects.keySet()) {
            if (!key.startsWith(strPrefix)) {
                continue;
            }
            String suffix = key.substring(prefLen);
            int slash = suffix.indexOf('/');
            if (slash < 0) {
                objectsList.add(PurePosixPath.from(key));
            } else {
                prefixesSet.add(PurePosixPath.from(strPrefix + suffix.substring(0, slash) + "/"));
            }
        }
        return new ShallowListing(objectsList, new ArrayList<>(prefixesSet));
    }

    @Override
    public boolean exists(PurePosixPath name) {
        return objects.containsKey(S3Names.validateName(name));
    }

    @Override
    public List<DeleteError> removeObjects(List<PurePosixPath> names) {
        // Validate every name up front so an invalid entry cannot leave a half-applied deletion.
        List<String> validated = names.stream().map(S3Names::validateName).toList();
        validated.forEach(objects::remove);
        return List.of();
    }
}
