import io
from contextlib import AbstractContextManager, contextmanager
from pathlib import PurePosixPath
from threading import RLock
from typing import BinaryIO, Iterable, Union

from streamerate import slist, sset
from streamerate import stream as sstream

from bucketbase import DeleteError
from bucketbase.ibucket import IBucket, ObjectStream, ShallowListing


class _NonClosingBytesIO(io.BytesIO):
    def close(self) -> None:  # do not actually close to allow final read
        pass

    def really_close(self) -> None:
        super().close()


class MemoryBucket(IBucket):
    """
    Implements IObjectStorage interface, but stores all objects in memory.
    This class is intended to be used for testing purposes only.
    """

    def __init__(self) -> None:
        self._objects: dict[str, bytes] = {}
        self._lock = RLock()

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
    def open_write(self, name: PurePosixPath | str) -> AbstractContextManager[BinaryIO]:
        """
        Returns a writable sink that accumulates bytes in memory; on close, stores the
        object under 'name'. Suitable for tests and small files.
        """
        _name = self._validate_name(name)

        sink = _NonClosingBytesIO()
        try:
            yield sink
        finally:
            # Attempt to read buffer regardless of prior close by pyarrow
            try:
                sink.flush()
            except Exception:
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
                    except Exception:
                        pass
            with self._lock:
                self._objects[_name] = content
