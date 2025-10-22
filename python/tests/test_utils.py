import io
import tempfile
import unittest

from bucketbase.utils import NonClosingStream


class TestNoOwnershipIO(unittest.TestCase):
    """Test NoOwnershipIO wrapper functionality."""

    def test_wrapper_close_does_not_close_base_stream(self):
        """Wrapper close marks only wrapper as closed, base remains open."""
        base = io.BytesIO(b"test data")
        wrapper = NonClosingStream(base)

        wrapper.close()

        self.assertTrue(wrapper.closed)
        self.assertFalse(base.closed)

    def test_write_operations_raise_after_wrapper_closed(self):
        """Write/flush/seek operations raise ValueError after wrapper closed."""
        base = io.BytesIO()
        wrapper = NonClosingStream(base)

        wrapper.write(b"initial")
        self.assertEqual(base.getvalue(), b"initial")

        wrapper.close()

        # Write raises after close
        with self.assertRaises(ValueError) as ctx:
            wrapper.write(b"after close")
        self.assertIn("closed", str(ctx.exception).lower())

        # Flush raises after close
        with self.assertRaises(ValueError) as ctx:
            wrapper.flush()
        self.assertIn("closed", str(ctx.exception).lower())

        # Seek raises after close
        with self.assertRaises(ValueError) as ctx:
            wrapper.seek(10)
        self.assertIn("closed", str(ctx.exception).lower())

        # Tell raises after close
        with self.assertRaises(ValueError) as ctx:
            wrapper.tell()
        self.assertIn("closed", str(ctx.exception).lower())

    def test_read_operations_raise_after_wrapper_closed(self):
        """Read operations raise ValueError after wrapper closed."""
        base = io.BytesIO(b"test data")
        wrapper = NonClosingStream(base)

        # Read works before close
        data = wrapper.read(4)
        self.assertEqual(data, b"test")

        wrapper.close()

        # Read raises after close
        with self.assertRaises(ValueError) as ctx:
            wrapper.read()
        self.assertIn("closed", str(ctx.exception).lower())

    def test_capability_checks_raise_after_wrapper_closed(self):
        """readable/writable/seekable/isatty raise ValueError after wrapper closed."""
        base = io.BytesIO(b"test")
        wrapper = NonClosingStream(base)

        # Before close - these work
        self.assertTrue(wrapper.readable())
        self.assertTrue(wrapper.writable())
        self.assertTrue(wrapper.seekable())
        self.assertFalse(wrapper.isatty())

        wrapper.close()

        # After close - these raise ValueError
        with self.assertRaises(ValueError) as ctx:
            wrapper.readable()
        self.assertIn("closed", str(ctx.exception).lower())

        with self.assertRaises(ValueError) as ctx:
            wrapper.writable()
        self.assertIn("closed", str(ctx.exception).lower())

        with self.assertRaises(ValueError) as ctx:
            wrapper.seekable()
        self.assertIn("closed", str(ctx.exception).lower())

        with self.assertRaises(ValueError) as ctx:
            wrapper.isatty()
        self.assertIn("closed", str(ctx.exception).lower())

    def test_context_manager_closes_wrapper_only(self):
        """Using wrapper as context manager closes wrapper but not base."""
        base = tempfile.NamedTemporaryFile(delete=False)
        try:
            with NonClosingStream(base) as wrapper:
                self.assertFalse(wrapper.closed)
                wrapper.write(b"data")

            # After context exit
            self.assertTrue(wrapper.closed)
            self.assertFalse(base.closed)
            base.seek(0)
            self.assertEqual(base.read(), b"data")
        finally:
            base.close()
            import os

            os.unlink(base.name)

    def test_closed_property_reflects_both_wrapper_and_base(self):
        """closed property returns True if wrapper OR base is closed."""
        base = io.BytesIO(b"test")
        wrapper = NonClosingStream(base)

        # Both open
        self.assertFalse(wrapper.closed)

        # Wrapper closed, base open
        wrapper.close()
        self.assertTrue(wrapper.closed)

        # Reset: new wrapper, close base
        base2 = io.BytesIO(b"test")
        wrapper2 = NonClosingStream(base2)
        base2.close()

        # Base closed, wrapper not explicitly closed
        self.assertTrue(wrapper2.closed)

    def test_writelines_raises_after_close(self):
        """writelines raises ValueError after wrapper closed."""
        base = io.BytesIO()
        wrapper = NonClosingStream(base)

        wrapper.writelines([b"line1\n", b"line2\n"])
        self.assertEqual(base.getvalue(), b"line1\nline2\n")

        wrapper.close()

        with self.assertRaises(ValueError) as ctx:
            wrapper.writelines([b"line3\n"])
        self.assertIn("closed", str(ctx.exception).lower())

        # Base unchanged
        self.assertEqual(base.getvalue(), b"line1\nline2\n")

    def test_iteration_raises_after_wrapper_closed(self):
        """Iterator methods raise after wrapper closed."""
        base = io.BytesIO(b"line1\nline2\n")
        wrapper = NonClosingStream(base)

        # Works before close
        wrapper_iter = iter(wrapper)
        first_line = next(wrapper_iter)
        self.assertEqual(first_line, b"line1\n")

        wrapper.close()

        # Raises after close - iter should raise
        with self.assertRaises(ValueError):
            iter(wrapper)

        # next on wrapper should raise
        with self.assertRaises(ValueError):
            next(wrapper)

        # next on the pre-close iterator should also raise
        with self.assertRaises(ValueError):
            next(wrapper_iter)

    def test_truncate_and_fileno_raise_after_wrapper_closed(self):
        """truncate and fileno raise after wrapper closed."""
        base = io.BytesIO(b"test data")
        wrapper = NonClosingStream(base)

        wrapper.close()

        with self.assertRaises(ValueError):
            wrapper.truncate(5)

        # fileno typically raises on BytesIO anyway, but check closed state
        with self.assertRaises((ValueError, io.UnsupportedOperation)):
            wrapper.fileno()

    def test_readinto_raises_after_wrapper_closed(self):
        """readinto raises after wrapper closed."""
        base = io.BytesIO(b"test data")
        wrapper = NonClosingStream(base)

        buffer = bytearray(4)
        n = wrapper.readinto(buffer)
        self.assertEqual(n, 4)
        self.assertEqual(buffer, bytearray(b"test"))

        wrapper.close()

        buffer2 = bytearray(4)
        with self.assertRaises(ValueError):
            wrapper.readinto(buffer2)

    def test_init_validates_attribute_requirements(self):
        """Constructor validates base is io.IOBase compatible."""
        with self.assertRaises(TypeError) as ctx:
            NonClosingStream("not a stream")  # type: ignore
        self.assertIn("base must be a stream-like object with ", str(ctx.exception))

    def test_duck_typed_io_accepted(self):
        """Duck-typed IO object with required methods is accepted."""

        class DuckTypedIO:
            def __init__(self):
                self.closed = False

            def close(self):
                pass

            def write(self, data):
                pass

            def flush(self):
                pass

        base = DuckTypedIO()
        # Should not raise TypeError
        wrapper = NonClosingStream(base, required_attrs=["close", "write", "flush", "closed"])
        self.assertIsNotNone(wrapper)

    def test_write_flush_sequence_after_close_raises(self):
        """Simulates PyArrow ParquetWriter close sequence after wrapper closed - now raises."""
        base = io.BytesIO()
        wrapper = NonClosingStream(base)

        # Normal write
        wrapper.write(b"data")
        wrapper.flush()
        self.assertEqual(base.getvalue(), b"data")

        # Close wrapper
        wrapper.close()

        # Simulate ParquetWriter.__del__ trying to close/flush - now raises
        with self.assertRaises(ValueError):
            wrapper.flush()

        with self.assertRaises(ValueError):
            wrapper.write(b"final")

        with self.assertRaises(ValueError):
            wrapper.flush()

        # Base unchanged
        self.assertEqual(base.getvalue(), b"data")
        self.assertFalse(base.closed)

    def test_multiple_wrapper_instances_on_same_base(self):
        """Multiple wrappers can wrap same base, each controls own closed state."""
        base = io.BytesIO()
        wrapper1 = NonClosingStream(base)
        wrapper2 = NonClosingStream(base)

        wrapper1.write(b"w1")
        wrapper2.write(b"w2")
        self.assertEqual(base.getvalue(), b"w1w2")

        wrapper1.close()
        self.assertTrue(wrapper1.closed)
        self.assertFalse(wrapper2.closed)

        # wrapper2 still works
        wrapper2.write(b"w2b")
        self.assertEqual(base.getvalue(), b"w1w2w2b")

        wrapper2.close()
        self.assertFalse(base.closed)

    def test_cached_methods_still_check_closed_state(self):
        """Cached method references still check closed state on every call."""
        base = io.BytesIO()
        wrapper = NonClosingStream(base)

        # Cache the method before closing
        cached_write = wrapper.write

        # Close the wrapper
        wrapper.close()

        # Cached method should still raise ValueError
        with self.assertRaises(ValueError) as ctx:
            cached_write(b"data")
        self.assertIn("closed", str(ctx.exception).lower())


if __name__ == "__main__":
    unittest.main()
