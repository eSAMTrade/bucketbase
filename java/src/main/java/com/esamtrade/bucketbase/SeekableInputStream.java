package com.esamtrade.bucketbase;

import java.io.IOException;
import java.io.InputStream;

/**
 * A readable stream whose position can be changed without reopening the object.
 */
public abstract class SeekableInputStream extends InputStream {
    public abstract long position() throws IOException;

    public abstract long size();

    public abstract void seek(long position) throws IOException;
}
