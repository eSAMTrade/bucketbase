package com.esamtrade.bucketbase;

import org.apache.commons.io.FilenameUtils;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Immutable, purely lexical POSIX path matching Python's standard-library
 * {@code pathlib.PurePosixPath} semantics (verified against the installed Python 3.14 stdlib).
 *
 * <p>In particular, empty paths render as {@code "."}, redundant separators and {@code "."}
 * components are removed, {@code ".."} components are preserved, a later absolute constructor
 * or join operand replaces everything before it, trailing separators are removed, and exactly
 * two leading separators are preserved. No method accesses the filesystem. Use
 * {@link PurePosixPrefix} when a trailing separator must remain significant for an object-store
 * prefix.</p>
 *
 * <p>The Java-only {@link #resolve(String)} methods are aliases for the equivalent lexical
 * {@link #join(String, String...)} operation.</p>
 *
 * @see <a href="https://docs.python.org/3/library/pathlib.html#pathlib.PurePosixPath">
 *     Python pathlib.PurePosixPath</a>
 */
public final class PurePosixPath implements Comparable<PurePosixPath> {

    public static final String SEP = "/";

    private final String root;
    private final String[] tail;
    private final String[] parts;

    public static PurePosixPath from(String path, String... more) {
        return new PurePosixPath(path, more);
    }

    public static PurePosixPath from(Path path) {
        Objects.requireNonNull(path, "path");
        return new PurePosixPath(FilenameUtils.separatorsToUnix(path.toString()));
    }

    public PurePosixPath() {
        this("");
    }

    public PurePosixPath(String first, String... more) {
        Objects.requireNonNull(first, "first");
        Objects.requireNonNull(more, "more");

        String[] paths = new String[more.length + 1];
        paths[0] = first;
        System.arraycopy(more, 0, paths, 1, more.length);
        ParsedPath parsed = parse(joinRawPaths(paths));
        this.root = parsed.root();
        this.tail = parsed.tail();
        this.parts = makeParts(root, tail);
    }

    /**
     * Constructs a path as though every array element were supplied as a separate Python
     * {@code PurePosixPath(*paths)} argument.
     */
    public PurePosixPath(String[] paths) {
        Objects.requireNonNull(paths, "paths");
        ParsedPath parsed = parse(joinRawPaths(paths));
        this.root = parsed.root();
        this.tail = parsed.tail();
        this.parts = makeParts(root, tail);
    }

    private PurePosixPath(ParsedPath parsed) {
        this.root = parsed.root();
        this.tail = parsed.tail();
        this.parts = makeParts(root, this.tail);
    }

    private static String joinRawPaths(String[] paths) {
        String result = "";
        for (String path : paths) {
            Objects.requireNonNull(path, "path segment");
            if (path.startsWith(SEP)) {
                result = path;
            } else if (result.isEmpty() || result.endsWith(SEP)) {
                result += path;
            } else {
                result += SEP + path;
            }
        }
        return result;
    }

    private static ParsedPath parse(String path) {
        if (path.isEmpty()) {
            return new ParsedPath("", new String[0]);
        }

        int leadingSeparators = 0;
        while (leadingSeparators < path.length() && path.charAt(leadingSeparators) == '/') {
            leadingSeparators++;
        }

        String root;
        if (leadingSeparators == 2) {
            root = "//";
        } else if (leadingSeparators > 0) {
            root = SEP;
        } else {
            root = "";
        }

        List<String> tail = new ArrayList<>();
        for (String part : path.substring(leadingSeparators).split(SEP, -1)) {
            if (!part.isEmpty() && !part.equals(".")) {
                tail.add(part);
            }
        }
        return new ParsedPath(root, tail.toArray(String[]::new));
    }

    private static String[] makeParts(String root, String[] tail) {
        if (root.isEmpty()) {
            return tail.clone();
        }
        String[] parts = new String[tail.length + 1];
        parts[0] = root;
        System.arraycopy(tail, 0, parts, 1, tail.length);
        return parts;
    }

    public PurePosixPath join(String other, String... more) {
        Objects.requireNonNull(other, "other");
        Objects.requireNonNull(more, "more");
        String[] paths = new String[more.length + 2];
        paths[0] = toString();
        paths[1] = other;
        System.arraycopy(more, 0, paths, 2, more.length);
        return new PurePosixPath(paths);
    }

    public PurePosixPath join(PurePosixPath other) {
        Objects.requireNonNull(other, "other");
        return new PurePosixPath(toString(), other.toString());
    }

    /**
     * Joins a prefix to this path. The result is a prefix because the final operand determines
     * whether a trailing separator is significant.
     */
    public PurePosixPrefix join(PurePosixPrefix other) {
        Objects.requireNonNull(other, "other");
        return new PurePosixPrefix(toString(), other.toString());
    }

    /** Converts this path to the equivalent object-store prefix. */
    public PurePosixPrefix toPrefix() {
        return PurePosixPrefix.from(this);
    }

    public PurePosixPath parent() {
        if (tail.length == 0) {
            return this;
        }
        return new PurePosixPath(
                new ParsedPath(root, Arrays.copyOf(tail, tail.length - 1)));
    }

    public String name() {
        return tail.length == 0 ? "" : tail[tail.length - 1];
    }

    public PurePosixPath resolve(String other) {
        return join(other);
    }

    public PurePosixPath resolve(PurePosixPath other) {
        return join(other);
    }

    public List<String> parts() {
        return List.of(parts);
    }

    public String get(int index) {
        return parts[index];
    }

    public String suffix() {
        String name = stripLeadingDots(name());
        int index = name.lastIndexOf('.');
        return index == -1 ? "" : name.substring(index);
    }

    public List<String> suffixes() {
        String name = stripLeadingDots(name());
        String[] split = name.split("\\.", -1);
        if (split.length <= 1) {
            return List.of();
        }
        List<String> suffixes = new ArrayList<>(split.length - 1);
        for (int i = 1; i < split.length; i++) {
            suffixes.add("." + split[i]);
        }
        return List.copyOf(suffixes);
    }

    public String stem() {
        String name = name();
        int index = name.lastIndexOf('.');
        if (index != -1) {
            String stem = name.substring(0, index);
            if (!stripLeadingDots(stem).isEmpty()) {
                return stem;
            }
        }
        return name;
    }

    private static String stripLeadingDots(String value) {
        int index = 0;
        while (index < value.length() && value.charAt(index) == '.') {
            index++;
        }
        return value.substring(index);
    }

    public boolean isAbsolute() {
        return !root.isEmpty();
    }

    public boolean isRelativeTo(PurePosixPath other) {
        Objects.requireNonNull(other, "other");
        if (!root.equals(other.root) || other.tail.length > tail.length) {
            return false;
        }
        for (int i = 0; i < other.tail.length; i++) {
            if (!tail[i].equals(other.tail[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        if (tail.length == 0) {
            return root.isEmpty() ? "." : root;
        }
        return root + String.join(SEP, tail);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof PurePosixPath that)) {
            return false;
        }
        return root.equals(that.root) && Arrays.equals(tail, that.tail);
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public int compareTo(PurePosixPath other) {
        Objects.requireNonNull(other, "other");
        String[] left = toString().split(SEP, -1);
        String[] right = other.toString().split(SEP, -1);
        int shared = Math.min(left.length, right.length);
        for (int i = 0; i < shared; i++) {
            int comparison = compareByCodePoint(left[i], right[i]);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    private static int compareByCodePoint(String left, String right) {
        int leftOffset = 0;
        int rightOffset = 0;
        while (leftOffset < left.length() && rightOffset < right.length()) {
            int leftCodePoint = left.codePointAt(leftOffset);
            int rightCodePoint = right.codePointAt(rightOffset);
            if (leftCodePoint != rightCodePoint) {
                return Integer.compare(leftCodePoint, rightCodePoint);
            }
            leftOffset += Character.charCount(leftCodePoint);
            rightOffset += Character.charCount(rightCodePoint);
        }
        return Integer.compare(left.length() - leftOffset, right.length() - rightOffset);
    }

    private record ParsedPath(String root, String[] tail) {
        private ParsedPath {
            tail = tail.clone();
        }

        @Override
        public String[] tail() {
            return tail.clone();
        }
    }
}
