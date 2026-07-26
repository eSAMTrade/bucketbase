package com.esamtrade.bucketbase;

import org.apache.commons.io.FilenameUtils;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Immutable, purely lexical POSIX prefix for object-store operations.
 *
 * <p>A prefix differs from {@link PurePosixPath} in two intentional ways: the empty prefix
 * renders as {@code ""}, and a trailing {@code "/"} is preserved. Consequently,
 * {@code new PurePosixPrefix("dir/")} and {@code new PurePosixPrefix("dir")} select different
 * sets of object keys. Redundant separators and {@code "."} components are otherwise normalized
 * with the same lexical rules as {@link PurePosixPath}, while {@code ".."} remains lexical.</p>
 *
 * <p>Conversions are explicit: {@link #toPath()} discards prefix-only trailing-separator
 * information, while {@link PurePosixPath#toPrefix()} creates a prefix without inventing a
 * trailing separator.</p>
 */
public final class PurePosixPrefix {

    public static final String SEP = "/";

    private final PurePosixPath path;
    private final boolean trailingSeparator;

    public static PurePosixPrefix from(String prefix, String... more) {
        return new PurePosixPrefix(prefix, more);
    }

    public static PurePosixPrefix from(Path prefix) {
        Objects.requireNonNull(prefix, "prefix");
        return new PurePosixPrefix(FilenameUtils.separatorsToUnix(prefix.toString()));
    }

    public static PurePosixPrefix from(PurePosixPath path) {
        return new PurePosixPrefix(path);
    }

    public PurePosixPrefix() {
        this("");
    }

    public PurePosixPrefix(String first, String... more) {
        Objects.requireNonNull(first, "first");
        Objects.requireNonNull(more, "more");

        String[] prefixes = new String[more.length + 1];
        prefixes[0] = first;
        System.arraycopy(more, 0, prefixes, 1, more.length);
        String raw = joinRaw(prefixes);
        PurePosixPath normalized = new PurePosixPath(raw);
        this.path = normalized;
        this.trailingSeparator = !isEmptyPath(normalized)
                && raw.endsWith(SEP)
                && !normalized.toString().endsWith(SEP);
    }

    public PurePosixPrefix(PurePosixPath path) {
        this(Objects.requireNonNull(path, "path"), false);
    }

    private PurePosixPrefix(PurePosixPath path, boolean trailingSeparator) {
        this.path = path;
        this.trailingSeparator = trailingSeparator
                && !isEmptyPath(path)
                && !path.toString().endsWith(SEP);
    }

    private static String joinRaw(String[] values) {
        String result = "";
        for (String value : values) {
            Objects.requireNonNull(value, "prefix segment");
            if (value.startsWith(SEP)) {
                result = value;
            } else if (result.isEmpty() || result.endsWith(SEP)) {
                result += value;
            } else {
                result += SEP + value;
            }
        }
        return result;
    }

    private static boolean isEmptyPath(PurePosixPath path) {
        return path.parts().isEmpty();
    }

    /** Joins textual path components to this prefix and returns an object path. */
    public PurePosixPath join(String other, String... more) {
        Objects.requireNonNull(other, "other");
        Objects.requireNonNull(more, "more");
        String[] values = new String[more.length + 2];
        values[0] = toString();
        values[1] = other;
        System.arraycopy(more, 0, values, 2, more.length);
        return new PurePosixPath(values);
    }

    /**
     * Joins textual prefix components and returns a prefix, preserving a trailing separator in
     * the last component.
     */
    public PurePosixPrefix joinPrefix(String other, String... more) {
        Objects.requireNonNull(other, "other");
        Objects.requireNonNull(more, "more");
        String[] values = new String[more.length + 2];
        values[0] = toString();
        values[1] = other;
        System.arraycopy(more, 0, values, 2, more.length);
        return new PurePosixPrefix(joinRaw(values));
    }

    /** Joins an object path to this prefix and returns an object path. */
    public PurePosixPath join(PurePosixPath other) {
        Objects.requireNonNull(other, "other");
        return new PurePosixPath(toString(), other.toString());
    }

    /** Joins another prefix and returns a prefix. */
    public PurePosixPrefix join(PurePosixPrefix other) {
        Objects.requireNonNull(other, "other");
        return new PurePosixPrefix(toString(), other.toString());
    }

    /**
     * Converts this prefix to a path. An empty prefix becomes the empty path ({@code "."}), and
     * any trailing separator is intentionally discarded.
     */
    public PurePosixPath toPath() {
        return path;
    }

    public boolean isEmpty() {
        return isEmptyPath(path);
    }

    public boolean hasTrailingSeparator() {
        return trailingSeparator;
    }

    public boolean isAbsolute() {
        return path.isAbsolute();
    }

    public String name() {
        return trailingSeparator || isEmpty() ? "" : path.name();
    }

    public List<String> parts() {
        if (!trailingSeparator) {
            return path.parts();
        }
        List<String> parts = new ArrayList<>(path.parts());
        parts.add("");
        return List.copyOf(parts);
    }

    public String get(int index) {
        return parts().get(index);
    }

    @Override
    public String toString() {
        if (isEmpty()) {
            return "";
        }
        return path + (trailingSeparator ? SEP : "");
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof PurePosixPrefix that)) {
            return false;
        }
        return trailingSeparator == that.trailingSeparator && path.equals(that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, trailingSeparator);
    }
}
