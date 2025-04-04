import io
import os
import re
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import PurePosixPath, Path
from typing import Tuple, Optional, Union, Iterable, BinaryIO

from pyxtension import PydanticValidated, validate
from streamerate import slist
from typing_extensions import Self

from bucketbase.errors import DeleteError

# Source: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
# As an exception - we won't allow "*" as a valid character in the name due to complications with the file systems
S3_NAME_CHARS_NO_SEP = r"\w!\-\.')("
S3_NAME_SAFE_RE = rf"^[{S3_NAME_CHARS_NO_SEP}][{S3_NAME_CHARS_NO_SEP}/]+$"

@dataclass(frozen=True)
class ShallowListing:
    """
    :param objects: list of object names, as PurePosixPath
    :param prefixes: list of prefixes (equivalent to directories on FileSystems) as strings, ending with "/"
    """
    objects: slist[PurePosixPath]
    prefixes: slist[str]

class ObjectStream:
    def __init__(self, stream: BinaryIO, name: PurePosixPath) -> None:
        self._stream = stream
        self._name = name

    def __enter__(self) -> BinaryIO:
        return self._stream

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._stream.close()

class IBucket(PydanticValidated, ABC):
    """
    This class is intended to be a base class for all object storage implementations.
    - it should not have any minio specific code
    - it should use only PurePosixPath as the object_name
    - it should not use bucket concept as it is not applicable to all object storage implementations.
        - Every instance of the this class will be associated with a single bucket for the lifetime of the instance.
    - No retries to the underlying storage (like Minio) can be used, since this should be done by the underlying Minio client
    """

    SEP = "/"
    SPLIT_PREFIX_RE = re.compile(rf"^((?:[{S3_NAME_CHARS_NO_SEP}]+/)*)([{S3_NAME_CHARS_NO_SEP}]*)$")
    OBJ_NAME_RE = re.compile(rf"^(?:[{S3_NAME_CHARS_NO_SEP}]+/)*[{S3_NAME_CHARS_NO_SEP}]+$")
    DEFAULT_ENCODING = "utf-8"
    MINIO_PATH_TEMP_SUFFIX_LEN = 43  # Minio will add to any downloaded path a `stat.etag + '.part.minio'` suffix
    WINDOWS_MAX_PATH = 260

    @staticmethod
    def _split_prefix(prefix: PurePosixPath | str) -> Tuple[Optional[str], Optional[str]]:
        """
        Validates & splits the given prefix into a "directory path" and a prefix.
        Throws ValueError if the prefix is invalid, thus this can be used to validate the prefix.

        :param prefix: prefix of objects to list. prefix can end with /, but use `str` as `PurePosixPath` will remove the trailing "/"
        :return: a tuple of (directory_path, name_prefix)
        """
        s_prefix = str(prefix)
        if s_prefix == "":
            return "", ""
        m = IBucket.SPLIT_PREFIX_RE.match(s_prefix)
        if m:
            dir_prefix = m.group(1) or ""
            name_prefix = m.group(2)
            assert isinstance(name_prefix, str)
            return dir_prefix, name_prefix
        raise ValueError(f"Invalid S3 prefix: {prefix}")

    @staticmethod
    def _encode_content(content: Union[str, bytes, bytearray]) -> bytes:
        validate(isinstance(content, (str, bytes, bytearray)), f"content must be str, bytes or bytearray, but got {type(content)}")
        return content if isinstance(content, (bytes, bytearray)) else content.encode(IBucket.DEFAULT_ENCODING)

    @staticmethod
    def _validate_name(name: PurePosixPath | str) -> str:
        """
        Validates the given object name.
        Throws ValueError if the object name is invalid, thus this can be used to validate the object name.

        Returns the object name as a string.
        """
        if isinstance(name, PurePosixPath):
            name = str(name)
        validate(IBucket.OBJ_NAME_RE.match(name), f"Invalid S3 object name: {name}")
        return name

    @abstractmethod
    def put_object(self, name: PurePosixPath | str, content: Union[str, bytes, bytearray]) -> None:
        raise NotImplementedError()

    @abstractmethod
    def put_object_stream(self, name: PurePosixPath | str, stream: BinaryIO) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get_object(self, name: PurePosixPath | str) -> bytes:
        """
        :raises FileNotFoundError: if the object is not found
        """
        raise NotImplementedError()

    @abstractmethod
    def get_object_stream(self, name: PurePosixPath | str) -> ObjectStream:
        """
        :raises FileNotFoundError: if the object is not found
        """
        raise NotImplementedError()

    @abstractmethod
    def get_size(self, name: PurePosixPath | str) -> int:
        raise NotImplementedError()

    def fput_object(self, name: PurePosixPath | str, file_path: Path) -> None:
        content = file_path.read_bytes()
        self.put_object(name, content)

    def fget_object(self, name: PurePosixPath | str, file_path: Path) -> None:
        random_suffix = uuid.uuid4().hex[:8]
        tmp_file_path = file_path.parent / f"_{file_path.name}.{random_suffix}.part"

        try:
            response = self.get_object(name)
            tmp_file_path.write_bytes(response)
            if os.path.exists(file_path):
                os.remove(file_path)  # For windows compatibility.
            os.rename(tmp_file_path, file_path)
        except FileNotFoundError as exc:
            if os.name == "nt":
                if len(str(tmp_file_path)) >= self.WINDOWS_MAX_PATH - self.MINIO_PATH_TEMP_SUFFIX_LEN:
                    raise ValueError(
                        "Reduce the Minio cache path length, Windows has limitation on the path length. "
                        "More details here: https://docs.python.org/3/using/windows.html#removing-the-max-path-limitation"
                    ) from exc
            raise

        finally:
            if tmp_file_path.exists():
                tmp_file_path.unlink(missing_ok=True)

    def remove_prefix(self, prefix: PurePosixPath | str) -> None:
        """
        Removes all objects with given prefix.
        """
        objects = self.list_objects(prefix)
        self.remove_objects(objects)

    @abstractmethod
    def list_objects(self, prefix: PurePosixPath | str = "") -> slist[PurePosixPath]:
        """
        Performs a deep/recursive listing of all objects with given prefix.

        :param prefix: prefix of objects to list. prefix can end with /, but use `str` as `PurePosixPath` will remove the trailing "/"
        """
        raise NotImplementedError()

    @abstractmethod
    def shallow_list_objects(self, prefix: PurePosixPath | str = "") -> ShallowListing:
        """
        Performs a shallow listing of all objects with given prefix.
        It will return a list of objects and a list of common prefixes (equivalent to directories on FileSystems).

        :param prefix: prefix of objects to list. prefix can end with /, but use `str` as `PurePosixPath` will remove the trailing "/"
        """
        raise NotImplementedError()

    @abstractmethod
    def exists(self, name: PurePosixPath | str) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def remove_objects(self, names: Iterable[PurePosixPath | str]) -> slist[DeleteError]:
        """
        This does not return an error when a specified file doesn't exist in the bucket
        It's by design and is consistent with the behavior of similar APIs in Amazon S3.
        This design choice is made for a few reasons: Idempotency, Simplification of Client Logic, Security and Privacy, etc..
        """
        raise NotImplementedError()

    def copy_prefix(self, dst_bucket: Self, src_prefix: PurePosixPath | str, dst_prefix: PurePosixPath | str = "", threads: int = 1) -> None:
        """
        Copies all objects with given src_prefix to the dst_prefix, from self to dest_bucket.
        """
        validate(threads > 0, "threads must be greater than 0")
        src_objects = self.list_objects(src_prefix).to_list()
        if not isinstance(dst_prefix, str):
            dst_prefix = str(dst_prefix)
        if not isinstance(src_prefix, str):
            src_prefix = str(src_prefix)
        src_pref_len = len(src_prefix)

        def _copy_object(src_obj: PurePosixPath | str) -> None:
            obj = str(src_obj)
            assert obj.startswith(src_prefix)
            name = dst_prefix + obj[src_pref_len:]
            if name.startswith("/"):
                name = name[1:]
            dst_bucket.put_object(name, self.get_object(src_obj))

        max_threads = max(1, min(threads, len(src_objects)))
        src_objects.fastmap(_copy_object, poolSize=max_threads).size()

    def move_prefix(self, dst_bucket: Self, src_prefix: PurePosixPath | str, dst_prefix: PurePosixPath | str = "", threads: int = 1) -> None:
        """
        Moves all objects with given src_prefix to the dst_prefix, from src_bucket to self.
        """
        self.copy_prefix(dst_bucket, src_prefix, dst_prefix, threads)
        self.remove_prefix(src_prefix)


class AbstractAppendOnlySynchronizedBucket(IBucket):
    """
    This class is useful for implementing a Bucket having a local FS cache and a remote storage, and the cache is shared between multiple processes,
    so we'll need to synchronize the access to the LocalFS cache.
    """

    def __init__(self, base_bucket: Self) -> None:
        self._base_bucket = base_bucket

    def put_object(self, name: PurePosixPath | str, content: Union[str, bytes, bytearray]) -> None:
        self._lock_object(name)
        try:
            self._base_bucket.put_object(name, content)
        finally:
            self._unlock_object(name)

    def put_object_stream(self, name: PurePosixPath | str, stream: BinaryIO) -> None:
        self._lock_object(name)
        try:
            self._base_bucket.put_object_stream(name, stream)
        finally:
            self._unlock_object(name)

    def get_object(self, name: PurePosixPath | str) -> bytes:
        if self.exists(name):
            return self._base_bucket.get_object(name)
        self._lock_object(name)
        try:
            content = self._base_bucket.get_object(name)
        finally:
            self._unlock_object(name)
        return content

    def get_size(self, name: PurePosixPath | str) -> int:
        return self._base_bucket.get_size(name)

    def get_object_stream(self, name: PurePosixPath | str) -> ObjectStream:
        if self.exists(name):
            return self._base_bucket.get_object_stream(name)
        self._lock_object(name)
        try:
            stream = self._base_bucket.get_object_stream(name)
        finally:
            self._unlock_object(name)
        return stream

    def list_objects(self, prefix: PurePosixPath | str = "") -> slist[PurePosixPath]:
        return self._base_bucket.list_objects(prefix)

    def shallow_list_objects(self, prefix: PurePosixPath | str = "") -> ShallowListing:
        return self._base_bucket.shallow_list_objects(prefix)

    def exists(self, name: PurePosixPath | str) -> bool:
        return self._base_bucket.exists(name)

    def remove_objects(self, names: Iterable[PurePosixPath | str]) -> slist[DeleteError]:
        raise io.UnsupportedOperation("remove_objects is not supported for AbstractAppendOnlySynchronizedBucket")

    @abstractmethod
    def _lock_object(self, name: PurePosixPath | str):
        raise NotImplementedError()

    @abstractmethod
    def _unlock_object(self, name: PurePosixPath | str):
        raise NotImplementedError()
