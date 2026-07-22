import io
import logging
import os
from pathlib import Path, PurePosixPath
from threading import RLock
from typing import BinaryIO, Iterable, Union

import certifi
import minio
import urllib3
from aicodesign import ai_blackbox
from minio import Minio
from minio.datatypes import Object
from minio.deleteobjects import DeleteError, DeleteObject
from minio.helpers import MAX_PART_SIZE, MIN_PART_SIZE, DictType
from multiminio import MultiMinio
from packaging import version as pkg_version
from pyxtension import validate
from streamerate import slist
from streamerate import stream as sstream
from typing_extensions import override

from bucketbase.ibucket import IBucket, ObjectStream, ShallowListing


@ai_blackbox()
class _MinioRangeReader(io.RawIOBase):
    @ai_blackbox()
    def __init__(self, minio_client: Minio, bucket_name: str, object_name: str, version_id: str | None = None) -> None:
        super().__init__()
        metadata = minio_client.stat_object(bucket_name, object_name, version_id=version_id)
        self._minio_client = minio_client
        self._bucket_name = bucket_name
        self._object_name = object_name
        self._version_id = version_id
        if metadata.size is None:
            raise IOError(f"Minio returned no size for {object_name}")
        self._size = metadata.size
        self._etag = metadata.etag
        self._position = 0
        self._lock = RLock()

    @override
    @ai_blackbox()
    def readable(self) -> bool:
        self._check_closed()
        return True

    @override
    @ai_blackbox()
    def seekable(self) -> bool:
        self._check_closed()
        return True

    @override
    @ai_blackbox()
    def tell(self) -> int:
        self._check_closed()
        with self._lock:
            return self._position

    @override
    @ai_blackbox()
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        self._check_closed()
        with self._lock:
            if whence == io.SEEK_SET:
                position = offset
            elif whence == io.SEEK_CUR:
                position = self._position + offset
            elif whence == io.SEEK_END:
                position = self._size + offset
            else:
                raise ValueError(f"Invalid whence: {whence}")
            if position < 0:
                raise ValueError(f"Negative seek position: {position}")
            self._position = position
            return position

    @override
    @ai_blackbox()
    def read(self, size: int = -1) -> bytes:
        self._check_closed()
        with self._lock:
            remaining = max(0, self._size - self._position)
            length = remaining if size is None or size < 0 else min(size, remaining)
            if length == 0:
                return b""

            request_headers: DictType | None = {"If-Match": f'"{self._etag}"'} if self._etag else None
            try:
                response = self._minio_client.get_object(
                    self._bucket_name,
                    self._object_name,
                    offset=self._position,
                    length=length,
                    request_headers=request_headers,
                    version_id=self._version_id,
                )
                try:
                    data = response.read()
                finally:
                    try:
                        response.close()
                    finally:
                        response.release_conn()
            except minio.error.S3Error as exc:
                if exc.code == "PreconditionFailed":
                    raise OSError(f"Object {self._object_name} changed while the stream was open") from exc
                raise

            if len(data) != length:
                raise IOError(f"Expected {length} bytes from {self._object_name} at offset {self._position}, but received {len(data)}")
            self._position += length
            return data

    @override
    @ai_blackbox()
    def readall(self) -> bytes:
        return self.read()

    @override
    @ai_blackbox()
    def readinto(self, buffer: bytearray | memoryview) -> int:
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)

    @ai_blackbox()
    def _check_closed(self) -> None:
        if self.closed:  # pylint: disable=using-constant-test
            raise ValueError("I/O operation on closed file")


@ai_blackbox()
class MinioObjectStream(ObjectStream):
    @ai_blackbox()
    def __init__(self, minio_client: Minio, *, bucket_name: str, object_name: PurePosixPath, read_buffer_size: int, version_id: str | None = None) -> None:
        raw_reader = _MinioRangeReader(minio_client, bucket_name, str(object_name), version_id=version_id)
        stream: BinaryIO = io.BufferedReader(raw_reader, buffer_size=read_buffer_size) if read_buffer_size > 0 else raw_reader
        super().__init__(stream, object_name)


def build_minio_client(  # pylint: disable=too-many-positional-arguments
    endpoints: str,
    access_key: str,
    secret_key: str,
    secure: bool = True,
    region: str | None = "us-east-1",
    conn_pool_size: int = 128,
    timeout: int = 5,
) -> Minio:
    """
    :param endpoints: comma separated list of endpoints
    :param access_key: access key
    :param secret_key: secret key
    :param secure: use SSL
    :param region: region
    :param conn_pool_size: connection pool size
    :param timeout: timeout in seconds
    """
    ca_certs = os.environ.get("SSL_CERT_FILE") or certifi.where()
    https_pool_manager = urllib3.PoolManager(
        timeout=timeout,
        maxsize=conn_pool_size,
        cert_reqs="CERT_REQUIRED",
        ca_certs=ca_certs,
        retries=urllib3.Retry(total=1, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504]),
    )
    # and a non-SSL http client
    http_pool_manager = urllib3.PoolManager(
        timeout=timeout,
        maxsize=conn_pool_size,
        retries=urllib3.Retry(total=1, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504]),
    )

    endpoints_list = endpoints.split(",")
    minio_clients = []
    for endpoint in endpoints_list:
        http_client = https_pool_manager if secure else http_pool_manager
        minio_client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region,
            http_client=http_client,
        )
        minio_clients.append(minio_client)
    if len(minio_clients) > 1:
        multi_minio_client = MultiMinio(clients=minio_clients, max_try_timeout=timeout)
        return multi_minio_client
    return minio_clients[0]


def _detect_minio_object_name_attribute() -> bool:
    """
    Detect which attribute name is used by inspecting the Object class.
    Returns True if .name should be used, False if .object_name. This is a hack to support both minio versions <=7.2.16 and >=7.2.17
    """
    # Check if Object class has 'name' as a direct attribute/property
    obj_class = Object
    # In newer versions, 'name' is a property; in older versions, 'object_name' is
    has_name = hasattr(obj_class, "name") and isinstance(getattr(obj_class, "name", None), property)
    has_object_name = hasattr(obj_class, "object_name") and isinstance(getattr(obj_class, "object_name", None), property)

    if has_name:
        return True
    if has_object_name:
        return False
    try:
        minio_version = pkg_version.parse(minio.__version__)
        # Version 7.2.17 introduced the breaking change
        return minio_version < pkg_version.parse("7.2.17")
    except (AttributeError, ValueError):
        # Fallback: try to detect from __init__ signature
        import inspect  # pylint: disable=import-outside-toplevel

        sig = inspect.signature(obj_class.__init__)
        return "name" in sig.parameters


class MinioBucket(IBucket):
    # Default part size for multipart uploads (16 MiB)
    # Increased from MinIO library default (5 MiB) for better performance and to allow for objects up to 160GiB
    DEFAULT_PART_SIZE = 16 * 1024 * 1024
    DEFAULT_READ_BUFFER_SIZE = 128 * 1024
    _USES_NAME_ATTRIBUTE: bool = _detect_minio_object_name_attribute()

    def __init__(
        self,
        bucket_name: str,
        minio_client: Minio,
        part_size: int | None = None,
        *,
        read_buffer_size: int = DEFAULT_READ_BUFFER_SIZE,
    ) -> None:
        if part_size is None:
            part_size = self.DEFAULT_PART_SIZE
        validate(
            MIN_PART_SIZE <= part_size <= MAX_PART_SIZE,
            f"part_size must be between {MIN_PART_SIZE} and {MAX_PART_SIZE}",
            exc=ValueError,
        )
        validate(
            isinstance(read_buffer_size, int) and not isinstance(read_buffer_size, bool) and read_buffer_size >= 0,
            "read_buffer_size must be a non-negative int",
            exc=ValueError,
        )
        self._minio_client = minio_client
        self._bucket_name = bucket_name
        self._part_size = part_size
        self._read_buffer_size = read_buffer_size

    @classmethod
    def _get_object_name(cls, obj: Object) -> str:
        object_name = getattr(obj, "name", None) if cls._USES_NAME_ATTRIBUTE else obj.object_name
        if object_name is None:
            raise ValueError("Minio object listing item has no object name")
        return object_name

    def get_object(self, name: PurePosixPath | str) -> bytes:
        _name = self._validate_name(name)
        try:
            return self._read_object(_name)
        except minio.error.S3Error as exc:
            if exc.code == "NoSuchKey":
                raise FileNotFoundError(f"Object {_name} not found in bucket {self._bucket_name} on Minio") from exc
            raise

    @ai_blackbox()
    def _read_object(self, name: str, *, version_id: str | None = None) -> bytes:
        response = self._minio_client.get_object(self._bucket_name, name, version_id=version_id)
        try:
            return response.read()
        finally:
            try:
                response.close()
            finally:
                response.release_conn()

    @ai_blackbox()
    def _open_object_stream(self, name: str, *, version_id: str | None = None) -> ObjectStream:
        return MinioObjectStream(
            self._minio_client,
            bucket_name=self._bucket_name,
            object_name=PurePosixPath(name),
            read_buffer_size=self._read_buffer_size,
            version_id=version_id,
        )

    def get_object_stream(self, name: PurePosixPath | str) -> ObjectStream:
        _name = self._validate_name(name)
        try:
            return self._open_object_stream(_name)
        except minio.error.S3Error as e:
            if e.code == "NoSuchKey":
                raise FileNotFoundError(f"Object {_name} not found in bucket {self._bucket_name} on Minio") from e
            raise

    def fget_object(self, name: PurePosixPath | str, file_path: Path) -> None:
        """
        Raises:
            minio.error.S3Error(): e.code
            RuntimeError() if the path is too long
        """
        _name = self._validate_name(name)
        try:
            self._minio_client.fget_object(self._bucket_name, _name, str(file_path))
        except FileNotFoundError as exc:
            self._validate_windows_path_length(str(file_path.resolve()), exc)
            raise

    def put_object(self, name: PurePosixPath | str, content: Union[str, bytes, bytearray]) -> None:
        _content = self._encode_content(content)
        _name = self._validate_name(name)
        f = io.BytesIO(_content)
        self._minio_client.put_object(
            bucket_name=self._bucket_name,
            object_name=_name,
            data=f,
            length=len(_content),
        )

    def put_object_stream(self, name: PurePosixPath | str, stream: BinaryIO) -> None:
        _name = self._validate_name(name)
        self._minio_client.put_object(
            bucket_name=self._bucket_name,
            object_name=_name,
            data=stream,
            length=-1,
            part_size=self._part_size,
        )

    def fput_object(self, name: PurePosixPath | str, file_path: Path) -> None:
        _name = self._validate_name(name)
        self._minio_client.fput_object(self._bucket_name, _name, str(file_path))

    def list_objects(self, prefix: PurePosixPath | str = "") -> slist[PurePosixPath]:
        self._split_prefix(prefix)  # validate prefix
        _prefix = str(prefix)
        listing_itr = self._minio_client.list_objects(bucket_name=self._bucket_name, prefix=_prefix, recursive=True)
        # Handle both old and new minio versions: newer versions use .name instead of .object_name
        object_names = sstream(listing_itr).map(self._get_object_name).map(PurePosixPath).to_list()
        return object_names

    def shallow_list_objects(self, prefix: PurePosixPath | str = "") -> ShallowListing:
        """
        Performs a non-recursive listing of all objects with given prefix.
        """
        self._split_prefix(prefix)  # validate prefix
        _prefix = str(prefix)
        listing_itr = self._minio_client.list_objects(bucket_name=self._bucket_name, prefix=_prefix, recursive=False)
        # Handle both old and new minio versions: newer versions use .name instead of .object_name
        object_names = sstream(listing_itr).map(self._get_object_name).to_list()
        prefixes = object_names.filter(lambda x: x.endswith("/")).to_list()
        objects = object_names.filter(lambda x: not x.endswith("/")).map(PurePosixPath).to_list()
        return ShallowListing(objects=objects, prefixes=prefixes)

    def exists(self, name: PurePosixPath | str) -> bool:
        _name = self._validate_name(name)
        try:
            self._minio_client.stat_object(self._bucket_name, _name)
            return True
        except minio.error.S3Error as e:
            if e.code == "NoSuchKey":
                return False
            logging.exception(f"Error checking if object exists: {e}")
            raise

    def remove_objects(self, names: Iterable[PurePosixPath | str]) -> slist[DeleteError]:
        delete_objects_stream = sstream(names).map(self._validate_name).map(DeleteObject)

        # the return value is a generator and if will not be converted to a list the deletion won't happen
        errors = slist(self._minio_client.remove_objects(self._bucket_name, delete_objects_stream))
        return errors

    def get_size(self, name: PurePosixPath | str) -> int:
        try:
            st = self._minio_client.stat_object(self._bucket_name, str(name))
            if st.size is None:
                raise IOError(f"Minio returned no size for {name}")
            return st.size
        except minio.error.S3Error as e:
            if e.code == "NoSuchKey":
                raise FileNotFoundError(f"Object {name} not found in bucket {self._bucket_name} on Minio") from e
            raise
