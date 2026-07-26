"""Executable oracle for the Java ``PurePosixPath`` port.

Yes, this deliberately tests Python's standard-library ``pathlib.PurePosixPath``,
which would normally be absurd in a BucketBase test suite. Its purpose is to keep
the expectations in Java's ``PurePosixPathTest`` tied to the actual stdlib
behavior of the Python version running the BucketBase tests.
"""

import sys
import unittest
from pathlib import PurePosixPath

# Python 3.14 left-strips leading dots from the name before looking for the extension
# separator, which changes `suffix`/`suffixes`/`stem` for names that end in a dot or start
# with several. See CPython `pathlib`: `name.rfind('.')` guarded by `0 < i < len(name) - 1`
# (<= 3.13) versus `name.lstrip('.').rfind('.')` (>= 3.14).
DOTS_STRIPPED_BEFORE_SUFFIX = sys.version_info >= (3, 14)

# Python 3.12 switched path ordering from the `parts` tuple (`_cparts`) to the path string
# split on "/" (`_parts_normcase`). That reverses how a preserved "//" root sorts against "/":
# as parts, "//" > "/"; as split strings, ("", "", "a") < ("", "a").
ORDERING_SPLITS_THE_PATH_STRING = sys.version_info >= (3, 12)


class TestPurePosixPathStdlibContract(unittest.TestCase):
    def test_construction_and_parts_match_stdlib(self) -> None:
        absolute = PurePosixPath("/home/user")
        self.assertEqual("/home/user", str(absolute))
        self.assertEqual(("/", "home", "user"), absolute.parts)

        relative = PurePosixPath("home", "user")
        self.assertEqual("home/user", str(relative))
        self.assertEqual(("home", "user"), relative.parts)

        empty = PurePosixPath("")
        self.assertEqual(".", str(empty))
        self.assertEqual((), empty.parts)
        self.assertEqual(empty, PurePosixPath())

    def test_redundant_separators_trailing_separators_and_dot_components_collapse(self) -> None:
        self.assertEqual("/home/user", str(PurePosixPath("/home/user/")))
        self.assertEqual("home/user", str(PurePosixPath("home//./user/.")))
        self.assertEqual(".", str(PurePosixPath("./")))
        self.assertEqual("/", str(PurePosixPath(".", "./home/./user", "/.")))

    def test_dot_dot_components_remain_lexical(self) -> None:
        self.assertEqual("/home/user/..", str(PurePosixPath("/home/user/..")))
        self.assertEqual("/../x", str(PurePosixPath("/../x")))
        self.assertEqual("/a/../../x", str(PurePosixPath("/a/../../x")))
        self.assertEqual("a/..", str(PurePosixPath("a/..")))
        self.assertEqual("../x", str(PurePosixPath("../x")))

    def test_later_absolute_segment_replaces_earlier_segments(self) -> None:
        self.assertEqual("/b", str(PurePosixPath("/a", "/b")))
        self.assertEqual("/", str(PurePosixPath("/home/user", "/")))

        base = PurePosixPath("/a")
        self.assertEqual("/b", str(base.joinpath("/b")))
        self.assertEqual("/b", str(base.joinpath(PurePosixPath("/b"))))

    def test_parent_matches_stdlib(self) -> None:
        self.assertEqual("/home", str(PurePosixPath("/home/user").parent))
        self.assertEqual("/", str(PurePosixPath("/home").parent))
        self.assertEqual("/", str(PurePosixPath("/").parent))
        self.assertEqual(".", str(PurePosixPath("leaf").parent))
        self.assertEqual(".", str(PurePosixPath("").parent))
        self.assertEqual("/home/user", str(PurePosixPath("/home/user/..").parent))

    def test_name_matches_stdlib(self) -> None:
        self.assertEqual("file.txt", PurePosixPath("/home/user/file.txt").name)
        self.assertEqual("..", PurePosixPath("/home/user/..").name)
        self.assertEqual("", PurePosixPath("/").name)
        self.assertEqual("", PurePosixPath("").name)

    def test_absolute_detection_matches_stdlib(self) -> None:
        self.assertTrue(PurePosixPath("/home/user").is_absolute())
        self.assertTrue(PurePosixPath("//server/share").is_absolute())
        self.assertFalse(PurePosixPath("home/user").is_absolute())
        self.assertFalse(PurePosixPath(".").is_absolute())

    def test_is_relative_to_matches_stdlib(self) -> None:
        self.assertTrue(PurePosixPath("/home/user").is_relative_to(PurePosixPath("/home")))
        self.assertFalse(PurePosixPath("/home/user").is_relative_to(PurePosixPath("/etc")))
        self.assertFalse(PurePosixPath("/a").is_relative_to(PurePosixPath("")))
        self.assertTrue(PurePosixPath("a").is_relative_to(PurePosixPath(".")))
        self.assertTrue(PurePosixPath("../a").is_relative_to(PurePosixPath("..")))

    def test_suffix_suffixes_and_stem_match_stdlib(self) -> None:
        archive = PurePosixPath("archive.tar.gz")
        self.assertEqual(".gz", archive.suffix)
        self.assertEqual([".tar", ".gz"], archive.suffixes)
        self.assertEqual("archive.tar", archive.stem)

        dot_file = PurePosixPath(".bashrc")
        self.assertEqual("", dot_file.suffix)
        self.assertEqual([], dot_file.suffixes)
        self.assertEqual(".bashrc", dot_file.stem)

        final_dot = PurePosixPath("name.")
        self.assertEqual("name.", final_dot.name)
        if DOTS_STRIPPED_BEFORE_SUFFIX:
            self.assertEqual(".", final_dot.suffix)
            self.assertEqual(["."], final_dot.suffixes)
            self.assertEqual("name", final_dot.stem)
        else:
            self.assertEqual("", final_dot.suffix)
            self.assertEqual([], final_dot.suffixes)
            self.assertEqual("name.", final_dot.stem)

        multiple_leading_dots = PurePosixPath("..bashrc")
        self.assertEqual([], multiple_leading_dots.suffixes)
        if DOTS_STRIPPED_BEFORE_SUFFIX:
            self.assertEqual("", multiple_leading_dots.suffix)
            self.assertEqual("..bashrc", multiple_leading_dots.stem)
        else:
            self.assertEqual(".bashrc", multiple_leading_dots.suffix)
            self.assertEqual(".", multiple_leading_dots.stem)

    def test_exactly_two_leading_separators_are_preserved(self) -> None:
        self.assertEqual("//server/share", str(PurePosixPath("//server/share")))
        self.assertEqual(("//", "server", "share"), PurePosixPath("//server/share").parts)
        self.assertEqual("/server/share", str(PurePosixPath("///server/share")))

    def test_equality_hashing_and_ordering_match_canonical_stdlib_paths(self) -> None:
        empty = PurePosixPath("")
        dot = PurePosixPath(".")
        self.assertEqual(empty, dot)
        self.assertEqual(hash(empty), hash(dot))
        self.assertEqual(PurePosixPath("a"), PurePosixPath("a/"))

        self.assertLess(PurePosixPath("a/b"), PurePosixPath("a-b"))
        self.assertLess(PurePosixPath("a"), PurePosixPath("a/b"))
        self.assertLess(PurePosixPath("/a"), PurePosixPath("a"))
        self.assertLess(PurePosixPath("\ue000"), PurePosixPath("\U0001f600"))

        # A preserved "//" root is a distinct path from "/" on every version, but the two sort
        # in opposite orders depending on how the interpreter compares paths.
        self.assertNotEqual(PurePosixPath("//a"), PurePosixPath("/a"))
        if ORDERING_SPLITS_THE_PATH_STRING:
            self.assertLess(PurePosixPath("//a"), PurePosixPath("/a"))
        else:
            self.assertLess(PurePosixPath("/a"), PurePosixPath("//a"))


if __name__ == "__main__":
    unittest.main()
