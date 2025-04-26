package com.esamtrade.bucketbase;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ParquetUtils {
    static class StreamInputFile implements InputFile {
        private final ObjectStream stream;
        private final long length;

        StreamInputFile(ObjectStream stream, long length) {
            this.stream = stream;
            this.length = length;
        }

        @Override
        public long getLength() throws IOException {
            return length;
        }

        @Override
        public SeekableInputStream newStream() throws IOException {
            return new SeekableInputStreamWrapper(stream.getStream());
        }
    }

    public static class SeekableInputStreamWrapper extends SeekableInputStream {
        private final InputStream in;
        private long pos = 0;
        private final int MARK_LIMIT = Integer.MAX_VALUE;

        /**
         * @param in the raw InputStream (e.g. S3ObjectInputStream)
         */
        public SeekableInputStreamWrapper(InputStream in) {
            this.in = in;
            in.mark(MARK_LIMIT);

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
                in.mark(MARK_LIMIT);
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

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            pos++;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public void close() throws IOException {
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

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }
}