package com.esamtrade.bucketbase;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PurePosixPrefixTest {

    @Test
    void emptyPrefixIsNotTheDotPath() {
        PurePosixPrefix empty = new PurePosixPrefix();
        assertEquals("", empty.toString());
        assertEquals(List.of(), empty.parts());
        assertEquals("", empty.name());
        assertTrue(empty.isEmpty());
        assertEquals(".", empty.toPath().toString());
    }

    @Test
    void trailingSeparatorIsPreservedAsAnEmptyFinalPart() {
        PurePosixPrefix prefix = new PurePosixPrefix("dir/");
        assertEquals("dir/", prefix.toString());
        assertEquals(List.of("dir", ""), prefix.parts());
        assertEquals("", prefix.get(1));
        assertEquals("", prefix.name());
        assertTrue(prefix.hasTrailingSeparator());
        assertNotEquals(prefix, new PurePosixPrefix("dir"));
    }

    @Test
    void pathConversionIsExplicitlyLossyOnlyForTrailingSeparator() {
        PurePosixPath path = new PurePosixPath("dir/file.txt");
        assertEquals(new PurePosixPrefix("dir/file.txt"), path.toPrefix());
        assertEquals(path.toPrefix(), new PurePosixPrefix(path));
        assertEquals(path, path.toPrefix().toPath());
        assertEquals(new PurePosixPath("dir"), new PurePosixPrefix("dir/").toPath());
    }

    @Test
    void joinResultTypeFollowsTheFinalOperandType() {
        PurePosixPrefix base = new PurePosixPrefix("tenant/");
        assertEquals(new PurePosixPath("tenant/data"), base.join("data/"));
        assertEquals(new PurePosixPrefix("tenant/data/"), base.joinPrefix("data/"));
        assertEquals(new PurePosixPrefix("tenant/data/"),
                base.join(new PurePosixPrefix("data/")));
        assertEquals(new PurePosixPath("tenant/data/file.parquet"),
                base.join(new PurePosixPath("data/file.parquet")));
        assertEquals(new PurePosixPrefix("tenant/data/"),
                new PurePosixPath("tenant").join(new PurePosixPrefix("data/")));
    }

    @Test
    void absoluteJoinOperandReplacesEarlierComponents() {
        PurePosixPrefix base = new PurePosixPrefix("tenant/");
        assertEquals(new PurePosixPrefix("/other/"), base.joinPrefix("/other/"));
        assertEquals(new PurePosixPath("/other/file"), base.join(new PurePosixPath("/other/file")));
        assertTrue(new PurePosixPrefix("/other/").isAbsolute());
        assertFalse(base.isAbsolute());
    }

    @Test
    void redundantSeparatorsAndDotsCollapseButDotDotRemainsLexical() {
        assertEquals(new PurePosixPrefix("a/b/"), new PurePosixPrefix("a//./b//"));
        assertEquals(new PurePosixPrefix("a/../b/"), new PurePosixPrefix("a/../b/"));
    }
}
