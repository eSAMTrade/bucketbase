import io
import logging
import os
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Iterable, Union

import certifi
import minio
import urllib3
from minio import Minio
from minio.datatypes import Object
from minio.deleteobjects import DeleteError, DeleteObject
from minio.helpers import MAX_PART_SIZE, MIN_PART_SIZE
from multiminio import MultiMinio
from packaging import version as pkg_version
from pyxtension import validate
from streamerate import slist
from streamerate import stream as sstream
from urllib3 import BaseHTTPResponse

from bucketbase.ibucket import IBucket, ObjectStream, ShallowListing


class MinioObjectStream(ObjectStream):
    def __init__(self, response: BaseHTTPResponse, object_name: PurePosixPath) -> None:
        # Wrap the BaseHTTPResponse to make it compatible with BinaryIO
        super().__init__(response, object_name)  # type: ignore[arg-type]
        self._response = response
        self._size = int(response.headers.get("content-length", -1))

    def __enter__(self) -> BinaryIO:
        return self._response  # type: ignore[return-value]

    def __exit__(self, exc_type: type | None, exc_val: BaseException | None, exc_tb: object | None) -> None:
        self._response.close()
        self._response.release_conn()


def build_minio_client(  # pylint: disable=too-many-positional-arguments
    endpoints: str, access_key: str, secret_key: str, secure: bool = True, region: str | None = "us-east-1", conn_pool_size: int = 128, timeout: int = 5
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
    _USES_NAME_ATTRIBUTE: bool = _detect_minio_object_name_attribute()

    def __init__(self, bucket_name: str, minio_client: Minio, part_size: int | None = None) -> None:
        if part_size is None:
            part_size = self.DEFAULT_PART_SIZE
        validate(MIN_PART_SIZE <= part_size <= MAX_PART_SIZE, f"part_size must be between {MIN_PART_SIZE} and {MAX_PART_SIZE}", exc=ValueError)
        self._minio_client = minio_client
        self._bucket_name = bucket_name
        self._part_size = part_size

    @classmethod
    def _get_object_name(cls, obj: Object) -> str:
        return obj.name if cls._USES_NAME_ATTRIBUTE else obj.object_name

    def get_object(self, name: PurePosixPath | str) -> bytes:
        with self.get_object_stream(name) as response:
            assert isinstance(response, BaseHTTPResponse), f"Expected IOBase, got {type(response)}"
            try:
                data = bytes()
                for buffer in response.stream(amt=1024 * 1024):
                    data += buffer
                return data
            finally:
                response.release_conn()

    def get_object_stream(self, name: PurePosixPath | str) -> ObjectStream:
        _name = self._validate_name(name)
        try:
            response: BaseHTTPResponse = self._minio_client.get_object(self._bucket_name, _name)
        except minio.error.S3Error as e:
            if e.code == "NoSuchKey":
                raise FileNotFoundError(f"Object {_name} not found in bucket {self._bucket_name} on Minio") from e
            raise

        _name_path = PurePosixPath(_name) if isinstance(_name, str) else _name
        return MinioObjectStream(response, _name_path)

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
        self._minio_client.put_object(bucket_name=self._bucket_name, object_name=_name, data=f, length=len(_content))

    def put_object_stream(self, name: PurePosixPath | str, stream: BinaryIO) -> None:
        _name = self._validate_name(name)
        self._minio_client.put_object(bucket_name=self._bucket_name, object_name=_name, data=stream, length=-1, part_size=self._part_size)

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
            return st.size
        except minio.error.S3Error as e:
            if e.code == "NoSuchKey":
                raise FileNotFoundError(f"Object {name} not found in bucket {self._bucket_name} on Minio") from e
            raise
