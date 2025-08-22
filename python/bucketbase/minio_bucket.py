import io
import logging
import os
import threading
import traceback
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path, PurePosixPath
from queue import Empty, Queue
from typing import BinaryIO, Iterable, Optional, Union

import certifi
import minio
import urllib3
from minio import Minio
from minio.datatypes import Object
from minio.deleteobjects import DeleteError, DeleteObject
from minio.helpers import MIN_PART_SIZE
from multiminio import MultiMinio
from streamerate import slist as slist
from streamerate import stream as sstream
from urllib3 import BaseHTTPResponse

from bucketbase.ibucket import IBucket, ObjectStream, ShallowListing


class MinioObjectStream(ObjectStream):
    def __init__(self, response: BaseHTTPResponse, object_name: PurePosixPath) -> None:
        super().__init__(response, object_name)
        self._response = response
        self._size = int(response.headers.get("content-length", -1))

    def __enter__(self) -> ObjectStream:
        return self._response

    def __exit__(self, exc_type: type | None, exc_val: BaseException | None, exc_tb: object | None) -> None:
        self._response.close()
        self._response.release_conn()


def build_minio_client(
    endpoints: str, access_key: str, secret_key: str, secure: bool = True, region: str | None = "custom", conn_pool_size: int = 128, timeout: int = 5
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
    PART_SIZE = MIN_PART_SIZE

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

    def put_object_stream(self, name: PurePosixPath | str, stream: BinaryIO) -> None:
        _name = self._validate_name(name)
        self._minio_client.put_object(bucket_name=self._bucket_name, object_name=_name, data=stream, length=-1, part_size=self.PART_SIZE)

    def fput_object(self, name: PurePosixPath | str, file_path: Path) -> None:
        _name = self._validate_name(name)
        self._minio_client.fput_object(self._bucket_name, _name, str(file_path))

    def list_objects(self, prefix: PurePosixPath | str = "") -> slist[PurePosixPath]:
        self._split_prefix(prefix)  # validate prefix
        _prefix = str(prefix)
        listing_itr = self._minio_client.list_objects(bucket_name=self._bucket_name, prefix=_prefix, recursive=True)
        object_names = sstream(listing_itr).map(Object.object_name.fget).map(PurePosixPath).to_list()
        return object_names

    def shallow_list_objects(self, prefix: PurePosixPath | str = "") -> ShallowListing:
        """
        Performs a non-recursive listing of all objects with given prefix.
        """
        self._split_prefix(prefix)  # validate prefix
        _prefix = str(prefix)
        listing_itr = self._minio_client.list_objects(bucket_name=self._bucket_name, prefix=_prefix, recursive=False)
        object_names = sstream(listing_itr).map(Object.object_name.fget).to_list()
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

    def _uploader(self, _name: str, reader: BinaryIO, exc_holder: list[BaseException | None]):
        buffered: Optional[io.BufferedReader] = None
        try:
            buffered = io.BufferedReader(reader, buffer_size=max(self.PART_SIZE, MIN_PART_SIZE))
            self._minio_client.put_object(
                bucket_name=self._bucket_name,
                object_name=_name,
                data=buffered,
                length=-1,
                part_size=max(self.PART_SIZE, MIN_PART_SIZE),
            )
        except BaseException as e:
            exc_holder[0] = e
        finally:
            try:
                if buffered is not None:
                    buffered.close()
            except Exception:
                pass

    @contextmanager
    def open_write2(self, name: PurePosixPath | str) -> AbstractContextManager[BinaryIO]:
        """
        Returns a BinaryIO writer that streams to MinIO using the SDK's multipart upload via
        put_object(length=-1, part_size=...). Implementation uses a bounded queue and a background
        thread that reads from the queue and feeds put_object. Memory is bounded by queue size * chunk size.
        """
        _name = self._validate_name(name)

        buffer_transfer_queue: Queue[Optional[bytes]] = Queue(maxsize=16)  # bounded queue
        closed_flag = [0]
        writer = _QueueWriter(buffer_transfer_queue)
        reader = _QueueReader(buffer_transfer_queue, closed_flag)

        shared_exc_holder: list[BaseException | None] = [None]

        t = threading.Thread(target=self._uploader, name=f"minio-upload-{_name}", daemon=True, args=(_name, reader, shared_exc_holder))
        t.start()

        try:
            yield writer
            writer.close()  # signal EOF
            t.join()
            if shared_exc_holder[0] is not None:
                raise shared_exc_holder[0]
        finally:
            try:
                writer.close()
            except Exception:
                pass


class _QueueWriter(io.RawIOBase):
    CHUNK_SIZE = 1024 * 1024  # 1 MiB per queue item

    def __init__(self, buffer_transfer_queue: Queue[Optional[bytes]]) -> None:
        super().__init__()
        self._buffer_transfer_queue = buffer_transfer_queue
        self._buffer = bytearray()

    def writable(self) -> bool:
        return True

    def write(self, b) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if not isinstance(b, (bytes, bytearray, memoryview)):
            b = bytes(b)

        self._buffer.extend(b)

        # Send complete chunks to queue
        while len(self._buffer) >= self.CHUNK_SIZE:
            chunk = bytes(self._buffer[: self.CHUNK_SIZE])
            self._buffer_transfer_queue.put(chunk)
            del self._buffer[: self.CHUNK_SIZE]

        return len(b)

    def flush(self) -> None:
        if self.closed:
            return
        # Send any remaining buffered data
        if self._buffer:
            chunk = bytes(self._buffer)
            self._buffer_transfer_queue.put(chunk)
            self._buffer.clear()

    def close(self) -> None:
        if not self.closed:
            try:
                self.flush()  # Flush remaining data first
                self._buffer_transfer_queue.put(None)  # EOF marker
            finally:
                super().close()


class _QueueReader(io.RawIOBase):
    def __init__(self, buffer_transfer_queue: Queue[Optional[bytes]], closed_flag: list[int]) -> None:
        self._buffer_transfer_queue = buffer_transfer_queue
        self._buffer = bytearray()
        self._eof = False
        self._closed_flag = closed_flag

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        if self._eof:
            return 0
        view = memoryview(b)
        total = 0
        while total < len(view):
            if self._buffer:
                n = min(len(self._buffer), len(view) - total)
                view[total : total + n] = self._buffer[:n]
                del self._buffer[:n]
                total += n
                if total:
                    break
            try:
                chunk = self._buffer_transfer_queue.get(timeout=0.5)
            except Empty:
                if self._closed_flag[0]:
                    self._eof = True
                    return 0 if total == 0 else total
                continue
            if chunk is None:
                self._eof = True
                return 0 if total == 0 else total
            self._buffer.extend(chunk)
        return total
