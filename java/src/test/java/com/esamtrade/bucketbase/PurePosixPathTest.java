package com.esamtrade.bucketbase;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract tests mirrored by {@code python/tests/test_pure_posix_path_stdlib_contract.py}.
 *
 * <p>The Python test runs these expectations against the installed stdlib
 * {@code pathlib.PurePosixPath}; this class applies the same expectations to the Java port.</p>
 */
class PurePosixPathTest {

    @Test
    void constructionAndPartsMatchStdlib() {
        PurePosixPath absolute = new PurePosixPath("/home/user");
        assertEquals("/home/user", absolute.toString());
        assertEquals(List.of("/", "home", "user"), absolute.parts());
        assertEquals("/", absolute.get(0));

        PurePosixPath relative = new PurePosixPath("home", "user");
        assertEquals("home/user", relative.toString());
        assertEquals(List.of("home", "user"), relative.parts());

        PurePosixPath empty = new PurePosixPath("");
        assertEquals(".", empty.toString());
        assertEquals(List.of(), empty.parts());
        assertEquals(new PurePosixPath(), empty);
    }

    @Test
    void redundantSeparatorsTrailingSeparatorsAndDotComponentsCollapse() {
        assertEquals("/home/user", new PurePosixPath("/home/user/").toString());
        assertEquals("home/user", new PurePosixPath("home//./user/.").toString());
        assertEquals(".", new PurePosixPath("./").toString());
        assertEquals("/", new PurePosixPath(".", "./home/./user", "/.").toString());
    }

    @Test
    void dotDotComponentsRemainLexical() {
        assertEquals("/home/user/..", new PurePosixPath("/home/user/..").toString());
        assertEquals("/../x", new PurePosixPath("/../x").toString());
        assertEquals("/a/../../x", new PurePosixPath("/a/../../x").toString());
        assertEquals("a/..", new PurePosixPath("a/..").toString());
        assertEquals("../x", new PurePosixPath("../x").toString());
    }

    @Test
    void laterAbsoluteSegmentReplacesEarlierSegments() {
        assertEquals("/b", new PurePosixPath("/a", "/b").toString());
        assertEquals("/", new PurePosixPath("/home/user", "/").toString());

        PurePosixPath base = new PurePosixPath("/a");
        assertEquals("/b", base.join("/b").toString());
        assertEquals("/b", base.join(new PurePosixPath("/b")).toString());
        assertEquals("/b", base.resolve("/b").toString());
    }

    @Test
    void parentMatchesStdlib() {
        assertEquals("/home", new PurePosixPath("/home/user").parent().toString());
        assertEquals("/", new PurePosixPath("/home").parent().toString());
        assertEquals("/", new PurePosixPath("/").parent().toString());
        assertEquals(".", new PurePosixPath("leaf").parent().toString());
        assertEquals(".", new PurePosixPath("").parent().toString());
        assertEquals("/home/user", new PurePosixPath("/home/user/..").parent().toString());
    }

    @Test
    void nameMatchesStdlib() {
        assertEquals("file.txt", new PurePosixPath("/home/user/file.txt").name());
        assertEquals("..", new PurePosixPath("/home/user/..").name());
        assertEquals("", new PurePosixPath("/").name());
        assertEquals("", new PurePosixPath("").name());
    }

    @Test
    void absoluteDetectionMatchesStdlib() {
        assertTrue(new PurePosixPath("/home/user").isAbsolute());
        assertTrue(new PurePosixPath("//server/share").isAbsolute());
        assertFalse(new PurePosixPath("home/user").isAbsolute());
        assertFalse(new PurePosixPath(".").isAbsolute());
    }

    @Test
    void isRelativeToMatchesStdlib() {
        assertTrue(new PurePosixPath("/home/user").isRelativeTo(new PurePosixPath("/home")));
        assertFalse(new PurePosixPath("/home/user").isRelativeTo(new PurePosixPath("/etc")));
        assertFalse(new PurePosixPath("/a").isRelativeTo(new PurePosixPath("")));
        assertTrue(new PurePosixPath("a").isRelativeTo(new PurePosixPath(".")));
        assertTrue(new PurePosixPath("../a").isRelativeTo(new PurePosixPath("..")));
    }

    @Test
    void suffixSuffixesAndStemMatchStdlib() {
        PurePosixPath archive = new PurePosixPath("archive.tar.gz");
        assertEquals(".gz", archive.suffix());
        assertEquals(List.of(".tar", ".gz"), archive.suffixes());
        assertEquals("archive.tar", archive.stem());

        PurePosixPath dotFile = new PurePosixPath(".bashrc");
        assertEquals("", dotFile.suffix());
        assertEquals(List.of(), dotFile.suffixes());
        assertEquals(".bashrc", dotFile.stem());

        // Python 3.14 treats a final dot as a valid suffix.
        PurePosixPath finalDot = new PurePosixPath("name.");
        assertEquals(".", finalDot.suffix());
        assertEquals(List.of("."), finalDot.suffixes());
        assertEquals("name", finalDot.stem());

        PurePosixPath multipleLeadingDots = new PurePosixPath("..bashrc");
        assertEquals("", multipleLeadingDots.suffix());
        assertEquals(List.of(), multipleLeadingDots.suffixes());
        assertEquals("..bashrc", multipleLeadingDots.stem());
    }

    @Test
    void exactlyTwoLeadingSeparatorsArePreserved() {
        assertEquals("//server/share", new PurePosixPath("//server/share").toString());
        assertEquals(List.of("//", "server", "share"),
                new PurePosixPath("//server/share").parts());
        assertEquals("/server/share", new PurePosixPath("///server/share").toString());
    }

    @Test
    void equalityHashingAndOrderingMatchCanonicalStdlibPaths() {
        PurePosixPath empty = new PurePosixPath("");
        PurePosixPath dot = new PurePosixPath(".");
        assertEquals(empty, dot);
        assertEquals(empty.hashCode(), dot.hashCode());
        assertEquals(new PurePosixPath("a"), new PurePosixPath("a/"));

        assertTrue(new PurePosixPath("a/b").compareTo(new PurePosixPath("a-b")) < 0);
        assertTrue(new PurePosixPath("a").compareTo(new PurePosixPath("a/b")) < 0);
        assertTrue(new PurePosixPath("/a").compareTo(new PurePosixPath("a")) < 0);
        assertTrue(new PurePosixPath("//a").compareTo(new PurePosixPath("/a")) < 0);
        assertTrue(new PurePosixPath("\uE000").compareTo(new PurePosixPath("\uD83D\uDE00")) < 0,
                "ordering must compare Unicode code points, as Python does");

        TreeSet<PurePosixPath> paths = new TreeSet<>(List.of(
                new PurePosixPath("/home/./user"),
                new PurePosixPath("/home/user"),
                new PurePosixPath("/home/user2")));
        assertEquals(2, paths.size());
    }

    @Test
    void arrayConstructorMatchesSeparateConstructorArguments() {
        PurePosixPath fromArray = new PurePosixPath(new String[]{"a/b", "c"});
        PurePosixPath fromArguments = new PurePosixPath("a/b", "c");
        assertEquals(fromArguments, fromArray);
        assertEquals(new PurePosixPath(fromArray.toString()), fromArray);
        assertTrue(new PurePosixPath(new String[]{"/a"}).isAbsolute());
    }

    @Test
    void javaSpecificFactoriesPreserveTheStdlibContract() {
        assertEquals("a/b", PurePosixPath.from("a", "b").toString());
        assertEquals("a/b", PurePosixPath.from(Path.of("a", "b")).toString());
        assertEquals("/", PurePosixPath.SEP);
    }

    @Test
    void valueIsImmutable() throws NoSuchFieldException {
        int classModifiers = PurePosixPath.class.getModifiers();
        int partsModifiers = PurePosixPath.class.getDeclaredField("parts").getModifiers();
        assertTrue(java.lang.reflect.Modifier.isFinal(classModifiers));
        assertTrue(java.lang.reflect.Modifier.isPrivate(partsModifiers));
        assertThrows(UnsupportedOperationException.class,
                () -> new PurePosixPath("a/b").parts().set(0, "changed"));
    }

    @Test
    void nullInputsAreRejected() {
        assertThrows(NullPointerException.class, () -> new PurePosixPath((String) null));
        assertThrows(NullPointerException.class, () -> new PurePosixPath((String[]) null));
        assertThrows(NullPointerException.class, () -> new PurePosixPath("a", (String[]) null));
    }
}
