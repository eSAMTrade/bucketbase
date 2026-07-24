package com.esamtrade.bucketbase;

import java.util.regex.Pattern;

/**
 * Object-name and prefix validation shared by every backend.
 *
 * <p>The character set mirrors the Python library's {@code S3_NAME_CHARS_NO_SEP}
 * ({@link IBucket#S3_NAME_CHARS_NO_SEP}) so keys written by one language are accepted by the
 * other. {@code \w} is matched with {@link Pattern#UNICODE_CHARACTER_CLASS} so non-ASCII keys
 * (e.g. {@code café.txt}) validate the same way they do under Python 3's Unicode-aware {@code re}.</p>
 */
final class S3Names {

    private static final Pattern OBJ_NAME_RE = Pattern.compile(
            "^(?:[" + IBucket.S3_NAME_CHARS_NO_SEP + "]+/)*[" + IBucket.S3_NAME_CHARS_NO_SEP + "]+$",
            Pattern.UNICODE_CHARACTER_CLASS);
    private static final Pattern PREFIX_RE = Pattern.compile(
            "^(?:[" + IBucket.S3_NAME_CHARS_NO_SEP + "]+/)*[" + IBucket.S3_NAME_CHARS_NO_SEP + "]*$",
            Pattern.UNICODE_CHARACTER_CLASS);

    private S3Names() {
    }

    static String validateName(PurePosixPath name) {
        return validateName(name.toString());
    }

    static String validateName(String name) {
        if (!OBJ_NAME_RE.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid S3 object name: " + name);
        }
        return name;
    }

    static String validatePrefix(PurePosixPrefix prefix) {
        String value = prefix.toString();
        if (!PREFIX_RE.matcher(value).matches()) {
            throw new IllegalArgumentException("Invalid S3 prefix: " + value);
        }
        return value;
    }
}
