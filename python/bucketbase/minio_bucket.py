import io
import logging
import os
import traceback
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Iterable, Union

import certifi
import minio
import urllib3
from minio import Minio
from minio.datatypes import Object
from minio.deleteobjects import DeleteError, DeleteObject
from multiminio import MultiMinio
from streamerate import slist, stream
from urllib3 import BaseHTTPResponse

from bucketbase.ibucket import IBucket, ObjectStream, ShallowListing


class MinioObjectStream(ObjectStream):
    def __init__(self, response: BaseHTTPResponse, object_name: PurePosixPath) -> None:
        super().__init__(response, object_name)
        self._response = response
        self._size = int(response.headers.get("content-length", -1))

    def __enter__(self) -> ObjectStream:
        return self._response

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._response.close()
        self._response.release_conn()


def build_minio_client(  # pylint: disable=too-many-arguments
    endpoints: str | None,
    access_key: str | None,
    secret_key: str | None,
    secure: bool = True,
    region: str | None = "custom",
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
    if not endpoints or not access_key or not secret_key:
        raise ValueError("Minio endpoints, access_key and secret_key must be provided")

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

    endpoints = endpoints.split(",")
    minio_clients = []
    for endpoint in endpoints:
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


class MinioBucket(IBucket):
    PART_SIZE = 5 * 1024 * 1024

    def __init__(self, bucket_name: str, minio_client: Minio) -> None:
        self._minio_client = minio_client
        self._bucket_name = bucket_name

    def get_object(self, name: PurePosixPath | str) -> bytes:
        with self.get_object_stream(name) as response:
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

        return MinioObjectStream(response, name)

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
            if os.name == "nt":
                destination_str = str(file_path.resolve())
                if len(destination_str) >= self.WINDOWS_MAX_PATH - self.MINIO_PATH_TEMP_SUFFIX_LEN:
                    raise RuntimeError(
                        "Reduce the Minio cache path length, Windows has limitation on the path length. "
                        "More details here: https://docs.python.org/3/using/windows.html#removing-the-max-path-limitation"
                    ) from exc
            raise

    def put_object(self, name: PurePosixPath | str, content: Union[str, bytes, bytearray]) -> None:
        _content = self._encode_content(content)
        _name = self._validate_name(name)
        f = io.BytesIO(_content)
        self._minio_client.put_object(bucket_name=self._bucket_name, object_name=_name, data=f, length=len(_content))

    def put_object_stream(self, name: PurePosixPath | str, data_stream: BinaryIO) -> None:  # pylint: disable=arguments-renamed
        _name = self._validate_name(name)
        self._minio_client.put_object(
            bucket_name=self._bucket_name,
            object_name=_name,
            data=data_stream,
            length=-1,
            part_size=self.PART_SIZE,
        )

    def fput_object(self, name: PurePosixPath | str, file_path: Path) -> None:
        _name = self._validate_name(name)
        self._minio_client.fput_object(self._bucket_name, _name, str(file_path))

    def list_objects(self, prefix: PurePosixPath | str = "") -> slist[PurePosixPath]:
        self._split_prefix(prefix)  # validate prefix
        _prefix = str(prefix)
        listing_itr = self._minio_client.list_objects(bucket_name=self._bucket_name, prefix=_prefix, recursive=True)
        object_names = stream(listing_itr).map(Object.object_name.fget).map(PurePosixPath).to_list()
        return object_names

    def shallow_list_objects(self, prefix: PurePosixPath | str = "") -> ShallowListing:
        """
        Performs a non-recursive listing of all objects with given prefix.
        """
        self._split_prefix(prefix)  # validate prefix
        _prefix = str(prefix)
        listing_itr = self._minio_client.list_objects(bucket_name=self._bucket_name, prefix=_prefix, recursive=False)
        object_names = stream(listing_itr).map(Object.object_name.fget).to_list()
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
            logging.exception(traceback.print_exc())
            raise

    def remove_objects(self, names: Iterable[PurePosixPath | str]) -> slist[DeleteError]:
        delete_objects_stream = stream(names).map(self._validate_name).map(DeleteObject)

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
