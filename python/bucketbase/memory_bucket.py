import io
import multiprocessing
import weakref
from contextlib import contextmanager
from multiprocessing.managers import DictProxy, SyncManager
from pathlib import PurePosixPath
from threading import RLock
from typing import Any, BinaryIO, Generator, Iterable, Optional, Union

from streamerate import slist, sset
from streamerate import stream as sstream

from bucketbase import DeleteError
from bucketbase.ibucket import IBucket, ObjectStream, ShallowListing


class _NonClosingBytesIO(io.BytesIO):
    def close(self) -> None:  # do not actually close to allow final read
        pass

    def really_close(self) -> None:
        super().close()


def _safe_manager_shutdown(manager: Any) -> None:
    """Helper to safely shut down a SyncManager during GC."""
    try:
        shutdown = getattr(manager, "shutdown", None)
        if callable(shutdown):
            shutdown()
    except Exception:  # pylint: disable=broad-exception-caught
        pass


class MemoryBucket(IBucket):
    """
    Implements IObjectStorage interface, but stores all objects in memory.
    This class is intended to be used for testing purposes only.
    """

    def __init__(self) -> None:
        self._objects: dict[str, bytes] | DictProxy[str, bytes] = {}
        self._lock: Any = RLock()
        self._manager: SyncManager | None = None
        self._owns_manager: bool = False

    @classmethod
    def create_shared(cls, manager: Optional[SyncManager] = None) -> "MemoryBucket":
        """
        Creates a MemoryBucket instance that is automatically shared across processes.

        If a 'manager' is provided, it uses it to create shared structures and the caller
        is responsible for the manager's lifecycle (shutdown).
        If not, it creates a new manager internally and registers a weakref finalizer
        to shut it down when the bucket is garbage collected.

        Note: The shared bucket's data lives only as long as the Manager process.
        Pickling a shared bucket preserves the proxy references, but unpickling after
        the Manager has been shut down will raise connection errors.
        """

        if manager is None:
            ctx = multiprocessing.get_context("spawn")
            manager = ctx.Manager()
            owns_manager = True
        else:
            owns_manager = False

        bucket = cls.__new__(cls)
        bucket._objects = manager.dict()
        bucket._lock = manager.RLock()
        bucket._manager = manager
        bucket._owns_manager = owns_manager

        if owns_manager:
            # Register a finalizer to shut down the manager when 'bucket' is GC-ed
            weakref.finalize(bucket, _safe_manager_shutdown, manager)

        return bucket

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_manager"] = None
        state["_owns_manager"] = False
        return state

    def put_object(self, name: PurePosixPath | str, content: Union[str, bytes, bytearray]) -> None:
        _name = self._validate_name(name)

        _content = self._encode_content(content)
        with self._lock:
            self._objects[_name] = _content

    def put_object_stream(self, name: PurePosixPath | str, stream: BinaryIO) -> None:
        _content = stream.read()
        self.put_object(name, _content)

    def get_object(self, name: PurePosixPath | str) -> bytes:
        _name = self._validate_name(name)

        with self._lock:
            if _name not in self._objects:
                raise FileNotFoundError(f"Object {_name} not found in MemoryObjectStore")
            return self._objects[_name]

    def get_object_stream(self, name: PurePosixPath | str) -> ObjectStream:
        content = self.get_object(name)
        return ObjectStream(io.BytesIO(content), PurePosixPath(name))

    def list_objects(self, prefix: PurePosixPath | str = "") -> slist[PurePosixPath]:
        self._split_prefix(prefix)  # validate prefix
        str_prefix = str(prefix)
        with self._lock:
            result = sstream(self._objects).filter(lambda obj: str(obj).startswith(str_prefix)).map(PurePosixPath).to_list()
        return result

    def shallow_list_objects(self, prefix: PurePosixPath | str = "") -> ShallowListing:
        self._split_prefix(prefix)  # validate prefix
        str_prefix = str(prefix)
        pref_len = len(str_prefix)
        objects = slist()
        prefixes = sset()
        with self._lock:
            for sobj in self.list_objects(prefix).map(str):
                if "/" not in sobj[pref_len:]:
                    objects.append(PurePosixPath(sobj))
                else:
                    suffix = sobj[pref_len:]
                    common_suffix = suffix.split("/", 1)[0]
                    common_prefix = str_prefix + common_suffix + "/"
                    prefixes.add(common_prefix)
        return ShallowListing(objects=objects, prefixes=prefixes.to_list())

    def exists(self, name: PurePosixPath | str) -> bool:
        _name = self._validate_name(name)
        with self._lock:
            return _name in self._objects

    def remove_objects(self, names: Iterable[PurePosixPath | str]) -> slist[DeleteError]:
        _list_of_objects = [str(obj) for obj in names]

        delete_errors = slist()
        with self._lock:
            for obj in _list_of_objects:
                obj = self._validate_name(obj)
                if obj in self._objects:
                    self._objects.pop(obj)
        return delete_errors

    def get_size(self, name: PurePosixPath | str) -> int:
        _name = self._validate_name(name)

        with self._lock:
            if _name not in self._objects:
                raise FileNotFoundError(f"Object {_name} not found in MemoryObjectStore")
            return len(self._objects[_name])  # Direct access to stored object

    @contextmanager
    def open_write_sync(self, name: PurePosixPath | str) -> Generator[BinaryIO, None, None]:
        """
        Synchronized version of the open_write, where we do not create any threads; This is intended to be used in performance critical paths.

        Returns a writable sink that accumulates bytes in memory; on close, stores the
        object under 'name'. Suitable for tests and small files.
        """
        _name = self._validate_name(name)

        sink = _NonClosingBytesIO()
        exception_occurred = False
        try:
            yield sink
        except BaseException:
            exception_occurred = True
            raise
        finally:
            # Attempt to read buffer regardless of prior close by pyarrow
            try:
                sink.flush()
            except Exception:  # pylint: disable=broad-exception-caught
                pass
            try:
                content = sink.getvalue()
            finally:
                # ensure we free memory
                if hasattr(sink, "really_close"):
                    sink.really_close()
                else:
                    try:
                        sink.close()
                    except Exception:  # pylint: disable=broad-exception-caught
                        pass
            # Only store content if no exception occurred
            if not exception_occurred:
                with self._lock:
                    self._objects[_name] = content
