import os
import threading
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path, PurePosixPath
from random import random
from time import sleep, time, time_ns
from typing import BinaryIO, Generator, Iterable, Optional, Union

from streamerate import slist

from bucketbase.errors import DeleteError
from bucketbase.ibucket import (
    AbstractAppendOnlySynchronizedBucket,
    IBucket,
    ObjectStream,
    ShallowListing,
)
from bucketbase.named_lock_manager import FileLockManager


class FSObjectStream(ObjectStream):
    def __init__(self, path: Path, name: PurePosixPath) -> None:
        # Initialize with a placeholder stream that will be replaced in __enter__
        super().__init__(None, name)  # type: ignore[arg-type]
        self._path = path

    def __enter__(self) -> BinaryIO:
        self._stream = self._path.open("rb")
        return self._stream

    def __exit__(self, exc_type: type | None, exc_val: BaseException | None, exc_tb: object | None) -> None:
        if self._stream:
            self._stream.close()
        self._stream = None  # type: ignore[assignment]


class FSBucket(IBucket):
    """
    Implements IObjectStorage interface, but stores all objects in local-mounted filesystem.

    Please note that this class will add a temporary directory to the "root" directory passed to the constructor.
    This directory will be used to store temporary files during the download process (for atomic rename operation), and the lock files.
    This directory will be created if it does not exist, and will be named as "$bucketbase.tmp".
    """

    BUFFER_SIZE = 128 * 1024  # ubuntu default readahead is 128k: cat /sys/block/<nvme>/queue/read_ahead_kb
    BUCKETBASE_TMP_DIR_NAME = "$bucketbase.tmp"  # this should contain an invalid S3 char
    TEMP_SEP = "#"  # this is an invalid S3 char

    def __init__(self, root: Path, timeout_ms: int = 5000) -> None:
        """
        :param root: the root directory of the bucket.
        :param timeout_ms: the timeout in milliseconds for the atomic rename operation.
        """
        assert isinstance(root, Path), f"root must be a Path, but got {type(root)}"
        if not root.exists():
            root.mkdir(parents=True, exist_ok=True)
        assert root.is_dir(), f"root must be a directory, but got {root}"
        self._root = root
        self._timeout_ms = timeout_ms

    def put_object(self, name: PurePosixPath | str, content: Union[str, bytes, bytearray]) -> None:
        stream = BytesIO(content) if isinstance(content, (bytes, bytearray)) else BytesIO(content.encode())
        self.put_object_stream(name, stream)

    def _get_tmp_obj_path(self, name: PurePosixPath | str) -> Path:
        tmp_obj_name = str(PurePosixPath(name)).replace(self.SEP, self.TEMP_SEP)
        return self._root / self.BUCKETBASE_TMP_DIR_NAME / f"{tmp_obj_name}@{str(int(time_ns()))}-{threading.get_ident()}.tmp"

    def put_object_stream(self, name: PurePosixPath | str, stream: BinaryIO) -> None:
        """
        Stores an object with the given name using a stream as content source in a synchronized manner.
        Prevents concurrent writes to the same object.

        :param name: Name of the object to store
        :param stream: Binary stream containing the object's content
        :raises ValueError: If name is invalid
        :raises io.UnsupportedOperation: If the object already exists
        :raises IOError: If stream operations fail, or if the object atomic renaming times out due to high concurrency.
        """
        _name = self._validate_name(name)
        _object_path = self._root / _name

        temp_obj_path = self._get_tmp_obj_path(name)
        try:
            temp_obj_path.parent.mkdir(parents=True, exist_ok=True)
            with temp_obj_path.open("wb") as f:
                while chunk := stream.read(self.BUFFER_SIZE):
                    # NOTE: Optimization possible: perform the read of the next chunk in async, while the current chunk is being written.
                    # See: https://github.com/eSAMTrade/bucketbase/issues/134
                    f.write(chunk)
            self._try_rename_tmp_file(temp_obj_path, _object_path)
        except FileNotFoundError as exc:
            # Clean up temporary file on failure
            temp_obj_path.unlink(missing_ok=True)
            self._validate_windows_path_length(_object_path, exc)
            raise
        except Exception:
            # Clean up temporary file on any other failure
            temp_obj_path.unlink(missing_ok=True)
            raise

    def _try_rename_tmp_file(self, tmp_file_path: Path, object_path: Path) -> None:
        object_path.parent.mkdir(parents=True, exist_ok=True)
        timeout_ms = time() + self._timeout_ms / 1000

        while time() < timeout_ms:
            try:
                os.replace(tmp_file_path, object_path)
                return
            except PermissionError:
                # sleep between 50 and 100 ms
                sleep(0.05 + 0.05 * random())
        raise IOError(f"Timeout renaming temp file {tmp_file_path} to {object_path}")

    @contextmanager
    def open_write(self, name: PurePosixPath | str, timeout_sec: Optional[float] = None) -> Generator[BinaryIO, None, None]:
        """
        Returns a writable sink that uses temporary files and atomic rename operations.
        Suitable for large files and ensures atomic writes to the filesystem.
        """
        _name = self._validate_name(name)
        _object_path = self._root / _name
        temp_obj_path = self._get_tmp_obj_path(name)

        try:
            temp_obj_path.parent.mkdir(parents=True, exist_ok=True)
            with temp_obj_path.open("wb") as sink:
                yield sink
            self._try_rename_tmp_file(temp_obj_path, _object_path)
        except BaseException:
            # Clean up temporary file if something goes wrong
            if temp_obj_path.exists():
                temp_obj_path.unlink(missing_ok=True)
            raise

    def get_object(self, name: PurePosixPath | str) -> bytes:
        """
        :raises FileNotFoundError: if the object is not found
        """
        _name = self._validate_name(name)
        _path = self._root / _name
        return _path.read_bytes()

    def get_object_stream(self, name: PurePosixPath | str) -> ObjectStream:
        _name = self._validate_name(name)
        fpath = self._root / _name
        if not fpath.exists() or not fpath.is_file():
            raise FileNotFoundError(f"Object {_name} not found in FSBucket")
        return FSObjectStream(fpath, PurePosixPath(name))

    def _get_recurs_listing(self, root: Path, s_prefix: str) -> slist[PurePosixPath]:
        listing = root.rglob("*")
        matching_objects = slist()
        for path in listing:
            # get the last part of the path relative to the self._root
            relative_path = path.relative_to(self._root)
            if relative_path.as_posix().startswith(s_prefix) and path.is_file():
                matching_objects.append(PurePosixPath(relative_path))
        return matching_objects

    def list_objects(self, prefix: PurePosixPath | str = "") -> slist[PurePosixPath]:
        """
        Performs a deep/recursive listing of all objects with given prefix.
        It will return the complete list of objects, even if they are in subdirectories, and even if the list is huge (it will perform pagination).

        :param prefix: prefix of objects to list. prefix can be empty ("") and end with /, but use `str` as `PurePosixPath` will remove the trailing "/"
        :raises ValueError: if the prefix is invalid
        """
        s_prefix = self._validate_prefix(prefix)

        do_exclude_tmp_dir = s_prefix == ""  # since the tmp dir starts with invalid char, any prefix won't match it
        dir_path, _ = self._split_prefix(s_prefix)

        start_list_lpath = self._root / dir_path

        # Here we do an optimization to avoid listing all files in the root of the ObjectStorage
        matching_objects = self._get_recurs_listing(start_list_lpath, s_prefix)

        if do_exclude_tmp_dir:
            return matching_objects.filter(lambda x: not x.as_posix().startswith(self.BUCKETBASE_TMP_DIR_NAME)).toList()

        return matching_objects

    def shallow_list_objects(self, prefix: PurePosixPath | str = "") -> ShallowListing:
        """
        Performs a non-recursive listing of all objects with given prefix.
        It will return a `ShallowListing` object, which contains a list of objects and a list of common prefixes (equivalent to directories on FileSystems).

        :param prefix: prefix of objects to list. prefix can be empty ("")end with /, but use `str` as `PurePosixPath` will remove the trailing "/"
        :raises ValueError: if the prefix is invalid
        """
        s_prefix = self._validate_prefix(prefix)
        dir_path, name_prefix = self._split_prefix(s_prefix)
        assert dir_path is not None and name_prefix is not None, "dir_path and name_prefix should not be None"
        start_list_lpath = self._root / dir_path

        listing = start_list_lpath.glob(name_prefix + "*")
        matching_objects = slist()
        prefixes = slist()
        for p in listing:
            if p.is_file():
                obj_path = PurePosixPath(p.relative_to(self._root))
                matching_objects.append(obj_path)
            elif p.is_dir():
                dir_path = p.relative_to(self._root).as_posix() + self.SEP
                if dir_path.startswith(self.BUCKETBASE_TMP_DIR_NAME):
                    continue
                prefixes.append(dir_path)
            else:
                raise ValueError(f"Unexpected path type: {p}")
        return ShallowListing(objects=matching_objects, prefixes=prefixes)

    def exists(self, name: PurePosixPath | str) -> bool:
        _name = self._validate_name(name)
        _obj_path = self._root / _name
        return _obj_path.exists() and _obj_path.is_file()

    def _try_remove_empty_dirs(self, p: Path) -> None:
        dir_to_remove = p.parent
        while dir_to_remove.relative_to(self._root).parts:
            try:
                dir_to_remove.rmdir()
            except OSError:
                break
            dir_to_remove = dir_to_remove.parent

    def remove_objects(self, names: Iterable[PurePosixPath | str]) -> slist[DeleteError]:
        """
        Note: Please bear in mind that this is not concurrent safe.
        Attention!!! The removal of objects is not atomic due to sequential removal of leftover directories.

        There's a way to make a sync version using FileLockForPath, but it will penalize the performance.
        """
        delete_errors = slist()
        for obj in names:
            obj = self._validate_name(obj)
            p = self._root / obj
            try:
                p.unlink(missing_ok=True)
            except Exception as e:  # pylint: disable=broad-except
                delete_errors.append(DeleteError(code="404", message=str(e), name=str(obj)))
            else:
                self._try_remove_empty_dirs(p)
        return delete_errors

    def get_root(self) -> Path:
        """This is not part of the IBucket interface, but it's useful for multiple purposes."""
        return self._root

    def get_size(self, name: PurePosixPath | str) -> int:
        return os.stat(self._root / name).st_size


class AppendOnlyFSBucket(AbstractAppendOnlySynchronizedBucket):
    """
    Intended to be used as a local FS cache(for remote bucket), shared between multiple processes, and the cache is append-only, synchronized with file locks.
    """

    def __init__(self, base: IBucket, locks_path: Path) -> None:
        """
        The locks_path should be a local file system path with write permissions.
        """
        super().__init__(base)
        self._locks_path = locks_path
        self._lock_manager = FileLockManager(locks_path)

    def _lock_object(self, name: PurePosixPath | str) -> None:
        lock = self._lock_manager.get_lock(name)
        lock.acquire()

    def _unlock_object(self, name: PurePosixPath | str) -> None:
        lock = self._lock_manager.get_lock(name, only_existing=True)
        lock.release()

    @classmethod
    def build(cls, root: Path, locks_path: Optional[Path] = None) -> "AppendOnlyFSBucket":
        if locks_path is None:
            locks_path = root / FSBucket.BUCKETBASE_TMP_DIR_NAME / "__locks__"
        return cls(FSBucket(root), locks_path)
