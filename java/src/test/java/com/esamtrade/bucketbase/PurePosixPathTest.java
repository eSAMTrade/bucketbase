package com.esamtrade.bucketbase;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PurePosixPathTest {

    @Test
    void nominal_test() {
        PurePosixPath basePath = new PurePosixPath("/home/user");
        assertEquals("/home/user", basePath.toString());
        assertEquals(Arrays.asList("", "home", "user"), Arrays.asList(basePath.parts));
        basePath = new PurePosixPath("home", "user");
        assertEquals("home/user", basePath.toString());
        assertEquals(Arrays.asList("home", "user"), Arrays.asList(basePath.parts));
    }

    @Test
    void slash_ending_path() {
        PurePosixPath basePath = new PurePosixPath("/home/user/");
        assertEquals("/home/user/", basePath.toString());
        assertEquals(Arrays.asList("", "home", "user", ""), Arrays.asList(basePath.parts));

        basePath = new PurePosixPath("/home/user", "/");
        assertEquals(Arrays.asList("", "home", "user", ""), Arrays.asList(basePath.parts));
        basePath = new PurePosixPath("home/user", "/");
        assertEquals(Arrays.asList("home", "user", ""), Arrays.asList(basePath.parts));

        basePath = new PurePosixPath("./home/user", "/.");
        assertEquals(Arrays.asList("home", "user", ""), Arrays.asList(basePath.parts));

        basePath = new PurePosixPath(".", "./home/./user", "/.");
        assertEquals(Arrays.asList("home", "user", ""), Arrays.asList(basePath.parts));
    }

    @Test
    void handling_updir_path() {
        PurePosixPath basePath = new PurePosixPath("/home/user/..");
        assertEquals("/home/", basePath.toString());
        assertEquals(Arrays.asList("", "home", ""), Arrays.asList(basePath.parts));

        basePath = new PurePosixPath("/home/../user", "/");
        assertEquals(Arrays.asList("", "user", ""), Arrays.asList(basePath.parts));

        basePath = new PurePosixPath("home/../user", "/");
        assertEquals(Arrays.asList("user", ""), Arrays.asList(basePath.parts));
    }

    @Test
    void join_appendsSinglePathCorrectly() {
        PurePosixPath basePath = new PurePosixPath("/home/user");
        PurePosixPath resultPath = basePath.join("docs");
        assertEquals("/home/user/docs", resultPath.toString());

        basePath = new PurePosixPath("/home/user/");
        resultPath = basePath.join("./docs/");
        assertEquals("/home/user/docs/", resultPath.toString());
    }

    @Test
    void join_appendsMultiplePathsCorrectly() {
        PurePosixPath basePath = new PurePosixPath("/home");
        PurePosixPath resultPath = basePath.join("user", "docs");
        assertEquals("/home/user/docs", resultPath.toString());

        basePath = new PurePosixPath("/home/");
        resultPath = basePath.join("./user/", "./../docs/./", "./last-path/.");
        assertEquals("/home/docs/last-path/", resultPath.toString());
    }

    @Test
    void parent_returnsParentPathForNonRoot() {
        PurePosixPath path = new PurePosixPath("/home/user/docs");
        assertEquals("/home/user", path.parent().toString());
    }

    @Test
    void parent_returnsRootForRootPath() {
        PurePosixPath path = new PurePosixPath("/");
        // expect to raise exception when call path.parent() on the root
        assertThrows(IllegalArgumentException.class, path::parent);

        path = new PurePosixPath("");
        assertThrows(IllegalArgumentException.class, path::parent);
    }

    @Test
    void name_returnsFileNameForFilePath() {
        PurePosixPath path = new PurePosixPath("/home/user/docs/file.txt");
        assertEquals("file.txt", path.name());
    }

    @Test
    void resolve_resolvesRelativePathAgainstBasePath() {
        PurePosixPath basePath = new PurePosixPath("/home/user");
        PurePosixPath resultPath = basePath.resolve("docs");
        assertEquals("/home/user/docs", resultPath.toString());
    }

    @Test
    void resolve_usesOtherPathIfAbsolute() {
        PurePosixPath basePath = new PurePosixPath("/home/user");
        PurePosixPath resultPath = basePath.resolve("/etc");
        assertEquals("/etc", resultPath.toString());
    }

    @Test
    void isAbsolute_identifiesAbsolutePath() {
        PurePosixPath path = new PurePosixPath("/home/user");
        assertTrue(path.isAbsolute());
    }

    @Test
    void isAbsolute_identifiesRelativePath() {
        PurePosixPath path = new PurePosixPath("home/user");
        assertFalse(path.isAbsolute());
    }

    @Test
    void isRelativeTo_identifiesSubPath() {
        PurePosixPath basePath = new PurePosixPath("/home");
        PurePosixPath otherPath = new PurePosixPath("/home/user");
        assertTrue(otherPath.isRelativeTo(basePath));
    }

    @Test
    void isRelativeTo_identifiesNonSubPath() {
        PurePosixPath basePath = new PurePosixPath("/home");
        PurePosixPath otherPath = new PurePosixPath("/etc");
        assertFalse(otherPath.isRelativeTo(basePath));
    }

    @Test
    void suffix_returnsFileExtension() {
        PurePosixPath path = new PurePosixPath("/home/user/file.tar.gz");
        assertEquals(".gz", path.suffix());
    }

    @Test
    void suffixes_returnsAllFileExtensions() {
        PurePosixPath path = new PurePosixPath("/home/user/archive.tar.gz");
        assertEquals(Arrays.asList(".tar", ".gz"), path.suffixes());
    }

    @Test
    void stem_returnsFileNameWithoutExtension() {
        PurePosixPath path = new PurePosixPath("/home/user/file.txt");
        assertEquals("file", path.stem());
    }

    @Test
    void get() {
        PurePosixPath path = new PurePosixPath("/home/user/file.txt");
        assertEquals("", path.get(0));
        assertEquals("home", path.get(1));
        assertEquals("user", path.get(2));
        assertEquals("file.txt", path.get(3));
    }

    @Test
    void testEquals() {
        PurePosixPath path1 = new PurePosixPath("/home/./user/file.txt");
        PurePosixPath path2 = new PurePosixPath("/home/user/./file.txt");
        assertEquals(path1, path2);
    }

    @Test
    void testHashCode() {
        PurePosixPath path1 = new PurePosixPath("/home/user/file.txt");
        PurePosixPath path2 = new PurePosixPath("/home/user/file.txt");
        assertEquals(path1.hashCode(), path2.hashCode());
    }

    @Test
    void invalidPath() {
        assertThrows(NullPointerException.class, () -> new PurePosixPath(null));
    }

    @Test
    void parent_ofSingleComponentAbsolutePathIsRoot() {
        assertEquals("/", new PurePosixPath("/home").parent().toString());
        assertEquals("/home", new PurePosixPath("/home/user").parent().toString());
        // A trailing slash does not change the parent.
        assertEquals("/home", new PurePosixPath("/home/user/").parent().toString());
    }

    @Test
    void compareToIsConsistentWithEquals() {
        // A "." mid-path collapses, so these normalize to the same parts: equals and compareTo
        // must agree.
        PurePosixPath a = new PurePosixPath("/home/./user");
        PurePosixPath b = new PurePosixPath("/home/user");
        assertEquals(a, b);
        assertEquals(0, a.compareTo(b));

        PurePosixPath c = new PurePosixPath("/home/user2");
        assertTrue(a.compareTo(c) < 0);
        assertTrue(c.compareTo(a) > 0);
        // A path type is routinely used as a sorted-map key; the ordering must be a total order
        // consistent with equals.
        java.util.TreeSet<PurePosixPath> set = new java.util.TreeSet<>(List.of(c, b, a));
        assertEquals(2, set.size());
    }

    @Test
    void sepIsStatic() {
        assertEquals("/", PurePosixPath.SEP);
    }

    @Test
    void trailingSlashAndDotComponentsAreCollapsed() {
        PurePosixPath basePath = new PurePosixPath("/home/user/");
        assertEquals("/home/user", basePath.toString());
        assertEquals(Arrays.asList("", "home", "user"), Arrays.asList(basePath.parts));

        basePath = new PurePosixPath("/home/user", "/");
        assertEquals("/", basePath.toString());
        assertTrue(basePath.isAbsolute());

        basePath = new PurePosixPath("home/user", "/");
        assertEquals("/", basePath.toString());
        assertTrue(basePath.isAbsolute());

        basePath = new PurePosixPath("./home/user", "/.");
        assertEquals("/", basePath.toString());
        assertTrue(basePath.isAbsolute());

        basePath = new PurePosixPath(".", "./home/./user", "/.");
        assertEquals("/", basePath.toString());
        assertTrue(basePath.isAbsolute());
    }

    @Test
    void dotDotComponentsArePreservedLexically() {
        PurePosixPath basePath = new PurePosixPath("/home/user/..");
        assertEquals("/home/user/..", basePath.toString());
        assertEquals(Arrays.asList("", "home", "user", ".."), Arrays.asList(basePath.parts));

        basePath = new PurePosixPath("/home/../user");
        assertEquals("/home/../user", basePath.toString());
        assertEquals(Arrays.asList("", "home", "..", "user"), Arrays.asList(basePath.parts));

        basePath = new PurePosixPath("home/../user");
        assertEquals("home/../user", basePath.toString());
        assertEquals(Arrays.asList("home", "..", "user"), Arrays.asList(basePath.parts));
    }


    @Test
    void dotDotIsLexicalAndMustNotRewriteAnObjectKeyOrLoseItsAnchor() {
        PurePosixPath absolute = new PurePosixPath("/../x");
        PurePosixPath joined = new PurePosixPath("tenant").join("../other/secret");

        assertAll(
                () -> assertEquals(
                        "/../x", absolute.toString(),
                        "PurePosixPath must preserve '..' instead of resolving it without filesystem context"),
                () -> assertTrue(
                        absolute.isAbsolute(),
                        "normalization must not turn an absolute path into a relative path"),
                () -> assertEquals(
                        "tenant/../other/secret", joined.toString(),
                        "joining an object name must not silently redirect it outside its prefix")
                 );
    }

    @Test
    void terminalDotMustNotCreateAnAbsolutePathOrAStoredTrailingSeparator() {
        PurePosixPath empty = new PurePosixPath("");
        PurePosixPath dot = new PurePosixPath(".");

        assertAll(
                () -> assertFalse(dot.isAbsolute(), "'.' is a relative path"),
                () -> assertEquals(
                        empty, dot,
                        "an empty PurePosixPath and '.' denote the same current-directory path"),
                () -> assertEquals(
                        "home/user", new PurePosixPath("home/user/.").toString(),
                        "a terminal '.' must be discarded, not converted to a trailing slash")
                 );
    }

    @Test
    void publicArrayConstructorMustUseTheSameComponentParsingAsTheVarargsConstructor() {
        PurePosixPath fromArray = new PurePosixPath(new String[]{"a/b", "c"});
        PurePosixPath fromVarargs = new PurePosixPath("a/b", "c");
        PurePosixPath reparsed = new PurePosixPath(fromArray.toString());
        PurePosixPath absoluteFromArray = new PurePosixPath(new String[]{"/a"});

        assertAll(
                () -> assertEquals(
                        fromVarargs, fromArray,
                        "equivalent public construction forms must create equal paths"),
                () -> assertEquals(
                        reparsed, fromArray,
                        "rendering and reparsing a path must preserve its value"),
                () -> assertEquals(
                        0, fromArray.compareTo(fromVarargs),
                        "equivalent construction forms must also compare as equal"),
                () -> assertTrue(
                        absoluteFromArray.isAbsolute(),
                        "an array element beginning with '/' must retain its absolute anchor")
                 );
    }

    @Test
    void dotFileNameMustNotBeTreatedAsAFileExtension() {
        PurePosixPath dotFile = new PurePosixPath(".bashrc");

        assertAll(
                () -> assertEquals("", dotFile.suffix()),
                () -> assertEquals(List.of(), dotFile.suffixes()),
                () -> assertEquals(".bashrc", dotFile.stem())
                 );
    }

    @Test
    void absoluteOperandsMustReplaceEarlierPathComponentsConsistently() {
        PurePosixPath base = new PurePosixPath("/a");
        PurePosixPath absolute = new PurePosixPath("/b");

        assertAll(
                () -> assertEquals(
                        "/b", base.join("/b").toString(),
                        "joining an absolute string must discard the earlier path"),
                () -> assertEquals(
                        "/b", base.join(absolute).toString(),
                        "joining an absolute PurePosixPath must discard the earlier path"),
                () -> assertEquals(
                        "/b", new PurePosixPath("/a", "/b").toString(),
                        "all public composition APIs must agree about absolute operands"),
                () -> assertEquals(
                        base.resolve("/b"), base.join("/b"),
                        "join and resolve must not give absolute operands conflicting meanings")
                 );
    }

    @Test
    void isRelativeToMustRequireCompatibleAbsoluteOrRelativeAnchors() {
        assertAll(
                () -> assertFalse(
                        new PurePosixPath("/a").isRelativeTo(new PurePosixPath("")),
                        "an absolute path cannot be relative to an unanchored empty path"),
                () -> assertTrue(
                        new PurePosixPath("a").isRelativeTo(new PurePosixPath(".")),
                        "a relative path is relative to the current-directory path")
                 );
    }

    @Test
    void parentOfSingleRelativeComponentIsTheCurrentPath() {
        PurePosixPath empty = new PurePosixPath("");
        PurePosixPath leaf = new PurePosixPath("leaf");

        assertEquals(
                empty, assertDoesNotThrow(leaf::parent),
                "the parent of a single relative component is the current path");
    }

    @Test
    void valueObjectMustNotExposeMutableStateThatDeterminesEqualityAndHashCode()
            throws NoSuchFieldException {
        int classModifiers = PurePosixPath.class.getModifiers();
        int partsModifiers = PurePosixPath.class.getDeclaredField("parts").getModifiers();

        assertAll(
                () -> assertTrue(
                        java.lang.reflect.Modifier.isFinal(classModifiers),
                        "the value class must not allow a subclass to mutate its hash state"),
                () -> assertTrue(
                        java.lang.reflect.Modifier.isPrivate(partsModifiers),
                        "the component array used by equals/hashCode must not be externally mutable")
                 );
    }

    @Test
    void doubleLeadingSlashMustNotBeCollapsedToASingleRootSlash() {
        assertEquals(
                "//server/share", new PurePosixPath("//server/share").toString(),
                "POSIX and pathlib preserve exactly two leading slashes");
    }
}
