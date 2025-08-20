package com.esamtrade.bucketbase;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class MemoryBucket extends BaseBucket {
    /**
     * Implements BaseBucket interface, but stores all objects in memory.
     * This class is intended to be used for testing purposes only.
     */

    private final Map<String, byte[]> objects;
    private final ReentrantLock lock;

    public MemoryBucket() {
        this.objects = new HashMap<>();
        this.lock = new ReentrantLock();
    }

    @Override
    public void putObject(PurePosixPath name, byte[] content) {
        String _name = validateName(name);
        byte[] _content = encodeContent(content);
        lock.lock();
        try {
            objects.put(_name, _content);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void putObjectStream(PurePosixPath name, InputStream stream) throws IOException {
        byte[] _content = stream.readAllBytes();
        putObject(name, _content);
    }

    @Override
    public byte[] getObject(PurePosixPath name) throws FileNotFoundException {
        String _name = validateName(name);
        lock.lock();
        try {
            if (!objects.containsKey(_name)) {
                throw new FileNotFoundException("Object " + _name + " not found in MemoryBucket");
            }
            return objects.get(_name);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ObjectStream getObjectStream(PurePosixPath name) throws FileNotFoundException {
        byte[] content = getObject(name);
        return new ObjectStream(new ByteArrayInputStream(content), name.toString());
    }

    @Override
    public long getSize(PurePosixPath name) throws IOException {
        byte[] content = getObject(name);
        return content.length;
    }

    @Override
    public List<PurePosixPath> listObjects(PurePosixPath prefix) {
        splitPrefix(prefix); // validate prefix
        String strPrefix = prefix.toString();
        lock.lock();
        try {
            return objects.keySet().stream()
                    .filter(obj -> obj.startsWith(strPrefix))
                    .map(PurePosixPath::from)
                    .collect(Collectors.toList());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ShallowListing shallowListObjects(PurePosixPath prefix) throws IOException {
        splitPrefix(prefix); // validate prefix
        String strPrefix = prefix.toString();
        int prefLen = strPrefix.length();
        List<PurePosixPath> objectsList = new ArrayList<>();
        Set<PurePosixPath> prefixesSet = new HashSet<>();
        lock.lock();
        try {
            for (PurePosixPath obj : listObjects(prefix)) {
                String sobj = obj.toString();
                if (!sobj.substring(prefLen).contains("/")) {
                    objectsList.add(obj);
                } else {
                    String suffix = sobj.substring(prefLen);
                    String commonSuffix = suffix.split("/", 2)[0];
                    String commonPrefix = strPrefix + commonSuffix + "/";
                    prefixesSet.add(PurePosixPath.from(commonPrefix));
                }
            }
        } finally {
            lock.unlock();
        }
        return new ShallowListing(objectsList, new ArrayList<>(prefixesSet));
    }

    @Override
    public boolean exists(PurePosixPath name) {
        String _name = validateName(name);
        lock.lock();
        try {
            return objects.containsKey(_name);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<DeleteError> removeObjects(List<PurePosixPath> names) {
        List<PurePosixPath> _listOfObjects = names.stream().collect(Collectors.toList());
        List<DeleteError> deleteErrors = new ArrayList<>();
        lock.lock();
        try {
            for (PurePosixPath obj : _listOfObjects) {
                String validatedObj = validateName(obj);
                objects.remove(validatedObj);
            }
        } finally {
            lock.unlock();
        }
        return deleteErrors;
    }
}
