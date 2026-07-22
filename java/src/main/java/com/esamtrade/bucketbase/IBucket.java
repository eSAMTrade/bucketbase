package com.esamtrade.bucketbase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public interface IBucket {
    String SEP = "/";
    int MINIO_PATH_TEMP_SUFFIX_LEN = 43;
    int WINDOWS_MAX_PATH = 260;
    String DEFAULT_ENCODING = "utf-8";

    void putObject(PurePosixPath name, byte[] content) throws IOException;

    void putObjectStream(PurePosixPath name, InputStream stream) throws IOException;

    byte[] getObject(PurePosixPath name) throws IOException;

    ObjectStream getObjectStream(PurePosixPath name) throws IOException;

    long getSize(PurePosixPath name) throws IOException;

    List<PurePosixPath> listObjects(PurePosixPath prefix) throws IOException;

    ShallowListing shallowListObjects(PurePosixPath prefix) throws IOException;

    boolean exists(PurePosixPath name) throws IOException;

    List<DeleteError> removeObjects(List<PurePosixPath> names) throws IOException;

    default void fputObject(PurePosixPath name, Path filePath) throws IOException {
        byte[] content = Files.readAllBytes(filePath);
        putObject(name, content);
    }

    default void fgetObject(PurePosixPath name, Path filePath) throws IOException {
        String randomSuffix = UUID.randomUUID().toString().substring(0, 8);
        Path tmpFilePath = filePath.getParent().resolve("_" + filePath.getFileName() + "." + randomSuffix + ".part");

        try {
            byte[] response = getObject(name);
            Files.write(tmpFilePath, response);
            Files.move(tmpFilePath, filePath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException exc) {
            if (System.getProperty("os.name").toLowerCase().contains("win")) {
                if (tmpFilePath.toString().length() >= WINDOWS_MAX_PATH - MINIO_PATH_TEMP_SUFFIX_LEN) {
                    throw new IllegalArgumentException(
                            "Reduce the Minio cache path length, Windows has limitation on the path length. " +
                                    "More details here: https://docs.python.org/3/using/windows.html#removing-the-max-path-limitation",
                            exc
                    );
                }
            }
            throw exc;
        } finally {
            Files.deleteIfExists(tmpFilePath);
        }
    }

    default void removePrefix(PurePosixPath prefix) throws IOException {
        List<PurePosixPath> objects = listObjects(prefix);
        removeObjects(objects);
    }

    default void copyPrefix(IBucket dstBucket, PurePosixPath srcPrefix, PurePosixPath dstPrefix, int threads) throws IOException {
        if (threads <= 0) {
            throw new IllegalArgumentException("threads must be greater than 0");
        }

        List<PurePosixPath> srcObjects = listObjects(srcPrefix);
        String srcPrefixStr = srcPrefix.toString();
        String dstPrefixStr = dstPrefix.toString();
        int srcPrefixLen = srcPrefixStr.length();

        ExecutorService executorService = Executors.newFixedThreadPool(Math.min(threads, srcObjects.size()));
        List<Future<Void>> futures = new ArrayList<>();

        for (PurePosixPath srcObj : srcObjects) {
            futures.add(executorService.submit(() -> {
                String objStr = srcObj.toString();
                if (!objStr.startsWith(srcPrefixStr)) {
                    return null;
                }

                String name = dstPrefixStr + objStr.substring(srcPrefixLen);
                if (name.startsWith("/")) {
                    name = name.substring(1);
                }
                dstBucket.putObject(PurePosixPath.from(name), getObject(srcObj));
                return null;
            }));
        }

        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (ExecutionException | InterruptedException e) {
                throw new IOException(e);
            }
        }

        executorService.shutdown();
    }

    default void movePrefix(IBucket dstBucket, PurePosixPath srcPrefix, PurePosixPath dstPrefix, int threads) throws IOException {
        copyPrefix(dstBucket, srcPrefix, dstPrefix, threads);
        removePrefix(srcPrefix);
    }
}