package com.esamtrade.bucketbase;

import java.io.IOException;
import java.io.InputStream;

public class ObjectStream implements AutoCloseable {
    private final SeekableInputStream stream;
    private final String name;

    public ObjectStream(InputStream stream, String name) {
        if (!(stream instanceof SeekableInputStream seekableStream)) {
            throw new IllegalArgumentException("Object streams must be seekable");
        }
        this.stream = seekableStream;
        this.name = name;
    }

    public SeekableInputStream getStream() {
        return stream;
    }

    public String getName() {
        return name;
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
