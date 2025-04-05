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
        """
        Encodes the given content to bytes using the default encoding if necessary.

        :param content: Content to encode, can be string, bytes or bytearray
        :return: Encoded content as bytes
        :raises ValueError: If content is not of type str, bytes or bytearray
        """
        validate(isinstance(content, (str, bytes, bytearray)), f"content must be str, bytes or bytearray, but got {type(content)}")
        return content if isinstance(content, (bytes, bytearray)) else content.encode(IBucket.DEFAULT_ENCODING)

    @staticmethod
    def _validate_name(name: PurePosixPath | str) -> str:
        """
        Validates the given object name.
        Throws ValueError if the object name is invalid, thus this can be used to validate the object name.

        :param name: Object name to validate
        :return: Validated object name as string
        :raises ValueError: If the object name is invalid
        """
        if isinstance(name, PurePosixPath):
            name = str(name)
        validate(IBucket.OBJ_NAME_RE.match(name), f"Invalid S3 object name: {name}")
        return name

    @abstractmethod
    def put_object(self, name: PurePosixPath | str, content: Union[str, bytes, bytearray]) -> None:
        """
        Stores an object with the given name and content.

        :param name: Name of the object to store
        :param content: Content to store, can be string, bytes or bytearray
        :raises ValueError: If name is invalid or content is of wrong type
        """
        raise NotImplementedError()

    @abstractmethod
    def put_object_stream(self, name: PurePosixPath | str, stream: BinaryIO) -> None:
        """
        Stores an object with the given name using a stream as content source.

        :param name: Name of the object to store
        :param stream: Binary stream containing the object's content
        :raises ValueError: If name is invalid
        :raises IOError: If stream operations fail
        """
        raise NotImplementedError()

    @abstractmethod
    def get_object(self, name: PurePosixPath | str) -> bytes:
        """
        Retrieves the content of an object.

        :param name: Name of the object to retrieve
        :return: Object content as bytes
        :raises FileNotFoundError: If the object is not found
        :raises ValueError: If name is invalid
        """
        raise NotImplementedError()

    @abstractmethod
    def get_object_stream(self, name: PurePosixPath | str) -> ObjectStream:
        """
        Retrieves a stream for reading the object's content.

        :param name: Name of the object to retrieve
        :return: ObjectStream instance for reading the content
        :raises FileNotFoundError: If the object is not found
        :raises ValueError: If name is invalid
        """
        raise NotImplementedError()

    @abstractmethod
    def get_size(self, name: PurePosixPath | str) -> int:
        """
        Gets the size of an object in bytes.

        :param name: Name of the object
        :return: Size of the object in bytes
        :raises FileNotFoundError: If the object is not found
        :raises ValueError: If name is invalid
        """
        raise NotImplementedError()

    def fput_object(self, name: PurePosixPath | str, file_path: Path) -> None:
        """
        Stores the content of a file as an object.

        :param name: Name of the object to store
        :param file_path: Path to the file to store
        :raises ValueError: If name is invalid
        :raises IOError: If file operations fail
        """
        content = file_path.read_bytes()
        self.put_object(name, content)

    def fget_object(self, name: PurePosixPath | str, file_path: Path) -> None:
        """
        Downloads an object's content to a file.

        :param name: Name of the object to retrieve
        :param file_path: Path where to save the object's content
        :raises FileNotFoundError: If the object is not found
        :raises ValueError: If name is invalid or path length exceeds Windows limitations
        :raises IOError: If file operations fail
        """
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
        Removes all objects with given prefix. Prefix can be empty ("") to remove all objects in the bucket.
        """
        objects = self.list_objects(prefix)
        self.remove_objects(objects)

    @abstractmethod
    def list_objects(self, prefix: PurePosixPath | str = "") -> slist[PurePosixPath]:
        """
        Performs a deep/recursive listing of all objects with given prefix.
        It will return the complete list of objects, even if they are in subdirectories, and event if the list is huge (it will perform pagination).

        :param prefix: prefix of objects to list. prefix can be empty ("")end with /, but use `str` as `PurePosixPath` will remove the trailing "/"
        :raises ValueError: if the prefix is invalid
        """
        raise NotImplementedError()

    @abstractmethod
    def shallow_list_objects(self, prefix: PurePosixPath | str = "") -> ShallowListing:
        """
        Performs a non-recursive listing of all objects with given prefix.
        It will return a `ShallowListing` object, which contains a list of objects and a list of common prefixes (equivalent to directories on FileSystems).

        :param prefix: prefix of objects to list. prefix can be empty ("")end with /, but use `str` as `PurePosixPath` will remove the trailing "/"
        :raises ValueError: if the prefix is invalid
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
    This class implements a synchronized, append-only bucket that wraps another bucket.
    It's useful for implementing a Bucket having a local FS cache and a remote storage, where the cache is shared between multiple processes,
    requiring synchronization of access to the LocalFS cache.

    Key characteristics:
    - Append-only: Objects cannot be modified once created
    - Synchronized: Uses locking to prevent concurrent access to the same object
    - Delegating: All operations are delegated to the underlying base bucket
    - No deletion: remove_objects operation is not supported

    The locking mechanism is implemented by the concrete subclasses through _lock_object and _unlock_object methods.
    """

    def __init__(self, base_bucket: IBucket) -> None:
        """
        Initialize the synchronized bucket with a base bucket.

        :param base_bucket: The underlying bucket to which operations will be delegated
        """
        self._base_bucket = base_bucket

    def put_object(self, name: PurePosixPath | str, content: Union[str, bytes, bytearray]) -> None:
        """
        Stores an object with the given name and content in a synchronized manner.
        Prevents concurrent writes to the same object.

        :param name: Name of the object to store
        :param content: Content to store, can be string, bytes or bytearray
        :raises ValueError: If name is invalid or content is of wrong type
        :raises io.UnsupportedOperation: If the object already exists
        """
        self._lock_object(name)
        try:
            self._base_bucket.put_object(name, content)
        finally:
            self._unlock_object(name)

    def put_object_stream(self, name: PurePosixPath | str, stream: BinaryIO) -> None:
        """
        Stores an object with the given name using a stream as content source in a synchronized manner.
        Prevents concurrent writes to the same object.

        :param name: Name of the object to store
        :param stream: Binary stream containing the object's content
        :raises ValueError: If name is invalid
        :raises io.UnsupportedOperation: If the object already exists
        :raises IOError: If stream operations fail
        """
        self._lock_object(name)
        try:
            self._base_bucket.put_object_stream(name, stream)
        finally:
            self._unlock_object(name)

    def get_object(self, name: PurePosixPath | str) -> bytes:
        """
        Retrieves the content of an object in a synchronized manner.
        Only locks if the object doesn't exist (to prevent race conditions during creation).

        :param name: Name of the object to retrieve
        :return: Object content as bytes
        :raises FileNotFoundError: If the object is not found
        :raises ValueError: If name is invalid
        """
        if self.exists(name):
            return self._base_bucket.get_object(name)
        self._lock_object(name)
        try:
            return self._base_bucket.get_object(name)
        finally:
            self._unlock_object(name)

    def get_size(self, name: PurePosixPath | str) -> int:
        return self._base_bucket.get_size(name)

    def get_object_stream(self, name: PurePosixPath | str) -> ObjectStream:
        """
        Retrieves a stream for reading the object's content in a synchronized manner.
        Only locks if the object doesn't exist (to prevent race conditions during creation).

        :param name: Name of the object to retrieve
        :return: ObjectStream instance for reading the content
        :raises FileNotFoundError: If the object is not found
        :raises ValueError: If name is invalid
        """
        if self.exists(name):
            return self._base_bucket.get_object_stream(name)
        self._lock_object(name)
        try:
            return self._base_bucket.get_object_stream(name)
        finally:
            self._unlock_object(name)

    def list_objects(self, prefix: PurePosixPath | str = "") -> slist[PurePosixPath]:
        """
        Lists all objects with the given prefix.
        No synchronization needed as this is a read-only operation.

        :param prefix: Prefix of objects to list
        :return: List of object names as PurePosixPath
        :raises ValueError: If prefix is invalid
        """
        return self._base_bucket.list_objects(prefix)

    def shallow_list_objects(self, prefix: PurePosixPath | str = "") -> ShallowListing:
        """
        Lists objects and prefixes at the current level with the given prefix.
        No synchronization needed as this is a read-only operation.

        :param prefix: Prefix of objects to list
        :return: ShallowListing containing objects and prefixes at the current level
        :raises ValueError: If prefix is invalid
        """
        return self._base_bucket.shallow_list_objects(prefix)

    def exists(self, name: PurePosixPath | str) -> bool:
        """
        Checks if an object exists.

        :param name: Name of the object to check
        :return: True if the object exists, False otherwise
        :raises ValueError: If name is invalid
        """
        return self._base_bucket.exists(name)

    def remove_objects(self, names: Iterable[PurePosixPath | str]) -> slist[DeleteError]:
        """
        Not supported in append-only buckets.

        :param names: Names of objects to remove
        :raises io.UnsupportedOperation: Always, as remove_objects is not supported
        """
        raise io.UnsupportedOperation("remove_objects is not supported for AbstractAppendOnlySynchronizedBucket")

    @abstractmethod
    def _lock_object(self, name: PurePosixPath | str) -> None:
        """
        Acquires a lock for the specified object.
        Must be implemented by concrete subclasses.

        :param name: Name of the object to lock
        """
        raise NotImplementedError()

    @abstractmethod
    def _unlock_object(self, name: PurePosixPath | str) -> None:
        """
        Releases the lock for the specified object.
        Must be implemented by concrete subclasses.

        :param name: Name of the object to unlock
        """
        raise NotImplementedError()
