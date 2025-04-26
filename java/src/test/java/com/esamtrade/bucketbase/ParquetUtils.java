package com.esamtrade.bucketbase;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ParquetUtils {
    static class SeekableByteArrayInputStream extends SeekableInputStream {
        private final byte[] data;
        private int pos;

        SeekableByteArrayInputStream(byte[] data) {
            this.data = data;
            this.pos = 0;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public void seek(long newPos) throws IOException {
            if (newPos < 0 || newPos > data.length) {
                throw new IOException("Invalid seek: " + newPos);
            }
            pos = (int) newPos;
        }

        @Override
        public int read() {
            if (pos >= data.length)
                return -1;
            return data[pos++] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (pos >= data.length)
                return -1;
            int toRead = Math.min(len, data.length - pos);
            System.arraycopy(data, pos, b, off, toRead);
            pos += toRead;
            return toRead;
        }

        @Override
        public void readFully(byte[] b) throws IOException {
            readFully(b, 0, b.length);
        }

        @Override
        public void readFully(byte[] b, int off, int len) throws IOException {
            int remaining = len;
            int offset = off;
            while (remaining > 0) {
                int r = read(b, offset, remaining);
                if (r < 0)
                    throw new EOFException("EOF before filling buffer");
                remaining -= r;
                offset += r;
            }
        }

        @Override
        public int read(ByteBuffer buf) throws IOException {
            if (!buf.hasRemaining())
                return 0;
            int toRead = Math.min(buf.remaining(), data.length - pos);
            if (toRead <= 0)
                return -1;
            buf.put(data, pos, toRead);
            pos += toRead;
            return toRead;
        }

        @Override
        public void readFully(ByteBuffer buf) throws IOException {
            int remaining = buf.remaining();
            byte[] temp = new byte[remaining];
            readFully(temp, 0, remaining);
            buf.put(temp);
        }
    }

    static class ByteArrayPositionOutputStream extends PositionOutputStream {
        private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        @Override
        public void write(int b) throws IOException {
            baos.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            baos.write(b, off, len);
        }

        @Override
        public long getPos() {
            return baos.size();
        }

        byte[] toByteArray() {
            return baos.toByteArray();
        }
    }

    static class InMemoryOutputFile implements OutputFile {
        private ByteArrayPositionOutputStream current;

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            current = new ByteArrayPositionOutputStream();
            return current;
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            return create(blockSizeHint);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }

        byte[] toByteArray() {
            return current.toByteArray();
        }
    }

    static class InMemoryInputFile implements InputFile {
        private final byte[] data;

        InMemoryInputFile(byte[] data) {
            this.data = data;
        }

        @Override
        public long getLength() {
            return data.length;
        }

        @Override
        public SeekableInputStream newStream() {
            return new SeekableByteArrayInputStream(data);
        }
    }

    static class BufferedBytesInputFile implements InputFile {
        private final byte[] data;

        BufferedBytesInputFile(byte[] data) {
            this.data = data;
        }

        @Override
        public long getLength() {
            return data.length;
        }

        @Override
        public SeekableInputStream newStream() {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            BufferedInputStream bis = new BufferedInputStream(bais);
            SeekableInputStream sis = new SeekableInputStreamWrapper(bis);
            return sis;
        }
    }

    static class StreamInputFile implements InputFile {
        private final ObjectStream stream;

        StreamInputFile(ObjectStream stream) {
            this.stream = stream;
        }

        @Override
        public long getLength() throws IOException {
            return stream.getStream().available();
        }

        @Override
        public SeekableInputStream newStream() throws IOException {
            return new SeekableInputStreamWrapper(stream.getStream());
        }
    }

    public static class SeekableInputStreamWrapper extends SeekableInputStream {
        private final InputStream in;
        private long pos = 0;
        private final int markLimit = 0;

        /**
         * @param in the raw InputStream (e.g. S3ObjectInputStream)
         */
        public SeekableInputStreamWrapper(InputStream in) {
            this.in = in;
            in.mark(markLimit);

        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public void seek(long newPos) throws IOException {
            if (newPos < 0) {
                throw new IOException("Cannot seek to negative position: " + newPos);
            }
            // if going backwards, reset to the mark and re‐mark
            if (newPos < pos) {
                in.reset();
                in.mark(markLimit);
                pos = 0;
            }
            // skip forward to the desired position
            long toSkip = newPos - pos;
            while (toSkip > 0) {
                long skipped = in.skip(toSkip);
                if (skipped <= 0) {
                    throw new EOFException("Unable to skip to position " + newPos);
                }
                toSkip -= skipped;
                pos += skipped;
            }
        }

        @Override
        public int read() throws IOException {
            int b = in.read();
            if (b >= 0)
                pos++;
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = in.read(b, off, len);
            if (n > 0)
                pos += n;
            return n;
        }

        @Override
        public void readFully(byte[] b, int off, int len) throws IOException {
            int total = 0;
            while (total < len) {
                int n = in.read(b, off + total, len - total);
                if (n < 0)
                    throw new EOFException("EOF before filling buffer");
                total += n;
            }
            pos += len;
        }

        @Override
        public void readFully(byte[] b) throws IOException {
            readFully(b, 0, b.length);
        }

        @Override
        public int read(ByteBuffer buf) throws IOException {
            int toRead = buf.remaining();
            byte[] tmp = new byte[toRead];
            int n = in.read(tmp, 0, toRead);
            if (n > 0) {
                buf.put(tmp, 0, n);
                pos += n;
            }
            return n;
        }

        @Override
        public void readFully(ByteBuffer buf) throws IOException {
            int toRead = buf.remaining();
            byte[] tmp = new byte[toRead];
            readFully(tmp, 0, toRead);
            buf.put(tmp);
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    public static class PositionOutputStreamWrapper extends PositionOutputStream {
        private final OutputStream out;
        private long pos = 0;

        public PositionOutputStreamWrapper(OutputStream out) {
            this.out = out;
        }

        @Override public void write(int b) throws IOException {
            out.write(b);
            pos++;
        }

        @Override public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            pos += len;
        }

        @Override public long getPos() {
            return pos;
        }

        @Override public void close() throws IOException {
            out.close();
        }
    }

    // 2) hands Parquet the above stream when it wants to create the file
    public static class OutputFileWrapper implements OutputFile {
        private final PositionOutputStreamWrapper posOut;

        public OutputFileWrapper(PositionOutputStreamWrapper posOut) {
            this.posOut = posOut;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return posOut;
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            return posOut;
        }

        @Override public boolean supportsBlockSize() { return false; }
        @Override public long defaultBlockSize()   { return 0; }
    }
}