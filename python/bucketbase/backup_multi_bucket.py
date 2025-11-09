import contextlib
import logging
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Iterable

try:
    _ = ExceptionGroup.__class__  # pylint: disable=used-before-assignment
except NameError:
    from exceptiongroup import ExceptionGroup

from streamerate import slist, sset
from typing_extensions import override

from bucketbase import DeleteError, IBucket, ShallowListing
from bucketbase.ibucket import ObjectStream


class BackupMultiBucket(IBucket):
    _DEFAULT_BUF_SIZE = 5 * 1024 * 1024  # 5MB

    def __init__(self, buckets: list[IBucket], timeout_sec: float) -> None:
        self._buckets = buckets
        self._timeout_sec = timeout_sec

    @override
    def put_object_stream(self, name: PurePosixPath | str, stream: BinaryIO) -> None:
        self._put_object_stream_to_missing(name, stream, self._buckets)

    @staticmethod
    def _raise_exc_if_fail(exceptions: list[Exception], exc: Exception | None = None) -> None:
        if exceptions:
            if len(exceptions) > 1:
                _msg = "Failed to write to one or more writers"
                if exc:
                    raise ExceptionGroup(_msg, exceptions) from exc  # pylint: disable=using-exception-groups-in-unsupported-version
                raise ExceptionGroup(_msg, exceptions)  # pylint: disable=using-exception-groups-in-unsupported-version
            if exc:
                raise exceptions[0] from exc
            raise exceptions[0]

    def _put_object_stream_to_missing(self, name: PurePosixPath | str, stream: BinaryIO, buckets: list[IBucket], size_hint: int | None = None) -> None:
        exceptions = []
        active_writer_bucket_contexts_by_id = {}
        for bucket in buckets:
            try:
                context = bucket.open_write(name, self._timeout_sec)
                writer = context.__enter__()  # pylint: disable=unnecessary-dunder-call
                active_writer_bucket_contexts_by_id[id(writer)] = (writer, context, bucket)
            except Exception as e:  # pylint: disable=broad-exception-caught
                exceptions.append(e)
        if not active_writer_bucket_contexts_by_id:
            self._raise_exc_if_fail(exceptions)
        with stream as _stream:
            while buf := _stream.read(self._DEFAULT_BUF_SIZE):
                assert active_writer_bucket_contexts_by_id, "Should have at least one active writer"
                for writer, context, _bucket in list(active_writer_bucket_contexts_by_id.values()):
                    try:
                        writer.write(buf)
                    except Exception as e:  # pylint: disable=broad-exception-caught
                        exceptions.append(e)
                        del active_writer_bucket_contexts_by_id[id(writer)]
                        with contextlib.suppress(Exception):  # Ignore context exit failures for failed writers
                            context.__exit__(e.__class__, e, e.__traceback__)

                        if not active_writer_bucket_contexts_by_id:
                            self._raise_exc_if_fail(exceptions)
                        logging.error(f"Failed to write to {writer}: {e}")
                        # Continue with remaining writers instead of raising exception immediately

        # Finalize all successful writers, collecting any finalization exceptions
        for _writer, context, bucket in active_writer_bucket_contexts_by_id.values():
            try:
                context.__exit__(None, None, None)
            except Exception as e:  # pylint: disable=broad-exception-caught
                exceptions.append(e)
            if size_hint is not None:
                try:
                    if bucket.get_size(name) != size_hint:
                        exceptions.append(FileExistsError(f"Object {name} has inconsistent sizes across buckets"))
                except Exception as e:  # pylint: disable=broad-exception-caught
                    exceptions.append(e)

        self._raise_exc_if_fail(exceptions)

    @override
    def fput_object(self, name: PurePosixPath | str, file_path: Path) -> None:
        """
        Uploads the file to all buckets that don't have it yet. If the file already exists in all buckets, it does nothing.
        At the end, it verifies that all buckets report the exact same size for the object. If not, it raises a FileExistsError with "inconsistent sizes"
        """
        try:
            file_size = file_path.stat().st_size
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"Source file not found: {file_path!s}") from exc

        needed_buckets = [bucket for bucket in self._buckets if not self._should_skip_upload(bucket, name, file_size)]
        if not needed_buckets:
            return
        with file_path.open("rb") as file:
            self._put_object_stream_to_missing(name, file, needed_buckets, size_hint=file_size)

    @staticmethod
    def _should_skip_upload(store: IBucket, obj_path: PurePosixPath | str, file_size: int) -> bool:
        """
        Checks if object already exists and has same size as the file to be uploaded.

        :return: True if the obj exists and has matching size (can be skipped), False if the obj doesn't exist

        :raises FileExistsError: If the object exists with a different size
        :raises OSError: If getting object size fails
        """
        try:
            remote_size = store.get_size(obj_path)
        except FileNotFoundError:
            return False
        except Exception as e:
            raise OSError(f"Failed to get object size from {obj_path}: {e}") from e

        if remote_size != file_size:
            raise FileExistsError(f"File {obj_path} already exists with different size (new: {file_size}, existing: {remote_size})")
        return True

    @override
    def get_object(self, name: PurePosixPath | str) -> bytes:
        """gets object content from first available storage, by it's full path"""
        last_not_found = None
        last_exception = None

        for bucket in self._buckets:
            try:
                return bucket.get_object(name)
            except FileNotFoundError as e:
                last_not_found = e
            except Exception as e:  # pylint: disable=broad-exception-caught
                last_exception = e
        if last_not_found is not None:
            raise last_not_found
        assert last_exception is not None
        raise last_exception

    @override
    def get_object_stream(self, name: PurePosixPath | str) -> ObjectStream:
        """gets object content stream from first available storage, by it's full path"""
        last_not_found = None
        last_exception = None

        for storage in self._buckets:
            try:
                return storage.get_object_stream(name)
            except FileNotFoundError as e:
                last_not_found = e
            except Exception as e:  # pylint: disable=broad-exception-caught
                last_exception = e
        if last_not_found is not None:
            raise last_not_found
        assert last_exception is not None
        raise last_exception

    @override
    def shallow_list_objects(self, prefix: PurePosixPath | str = "") -> ShallowListing:
        all_objects = sset()
        all_prefixes = sset()
        last_exc = None
        at_least_one_bucket = False
        for storage in self._buckets:
            try:
                shallow_listing = storage.shallow_list_objects(prefix=prefix)
                all_objects.update(shallow_listing.objects)
                all_prefixes.update(shallow_listing.prefixes)
                at_least_one_bucket = True
            except Exception as e:  # pylint: disable=broad-exception-caught
                last_exc = e
        if not at_least_one_bucket:
            assert last_exc is not None
            raise last_exc
        return ShallowListing(objects=all_objects.sorted(key=lambda x: x.name).to_list(), prefixes=all_prefixes.sorted().to_list())

    @override
    def exists(self, name: PurePosixPath | str) -> bool:
        """Returns True if the object exists AT LEAST in one of the buckets"""
        last_exc = None
        for bucket in self._buckets:
            try:
                if bucket.exists(name):
                    return True
            except Exception as e:  # pylint: disable=broad-exception-caught
                last_exc = e
                continue
        if last_exc is not None:
            raise last_exc
        return False

    def copy_object_from(self, src_bucket: IBucket, src_name: PurePosixPath | str, dst_name: PurePosixPath | str) -> None:
        try:
            src_object_size = src_bucket.get_size(src_name)
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"Source file not found: {src_name!s}") from exc

        needed_buckets = [bucket for bucket in self._buckets if not self._should_skip_upload(bucket, dst_name, src_object_size)]
        if not needed_buckets:
            return
        with src_bucket.get_object_stream(src_name) as file:
            self._put_object_stream_to_missing(dst_name, file, needed_buckets, size_hint=src_object_size)

    @override
    def put_object(self, name: PurePosixPath | str, content: str | bytes | bytearray) -> None:
        raise NotImplementedError()

    @override
    def get_size(self, name: PurePosixPath | str) -> int:
        raise NotImplementedError()

    @override
    def list_objects(self, prefix: PurePosixPath | str = "") -> slist[PurePosixPath]:
        raise NotImplementedError()

    @override
    def remove_objects(self, names: Iterable[PurePosixPath | str]) -> slist[DeleteError]:
        raise NotImplementedError()
