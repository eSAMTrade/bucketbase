import io
from pathlib import Path, PurePosixPath
from typing import Iterable, Union, BinaryIO

from streamerate import slist

from bucketbase.errors import DeleteError
from bucketbase.fs_bucket import AppendOnlyFSBucket
from bucketbase.ibucket import ShallowListing, IBucket, ObjectStream
from bucketbase import namedlock


class CachedImmutableBucket(IBucket):
    def __init__(self, cache: IBucket, main: IBucket, lock_manager: namedlock.NamedLockManager = None) -> None:
        self._cache = cache
        self._main = main
        self._lock_manager = lock_manager or namedlock.ThreadLockManager()

    def _get_with_cache_fallback(self, name:PurePosixPath | str, get_cache_func, fetch_and_cache_func):
        """Generic method for getting objects with cache fallback"""
        name_str = str(name)
        try:
            return get_cache_func(name)
        except FileNotFoundError:
            with self._lock_manager.get_lock(name_str):
                try:
                    return get_cache_func(name)  # Check cache again within lock
                except FileNotFoundError:
                    return fetch_and_cache_func(name)

    def get_object(self, name: PurePosixPath | str) -> bytes:
        def fetch_and_cache(n):
            content = self._main.get_object(n)
            self._cache.put_object(n, content)
            return content

        return self._get_with_cache_fallback(name, self._cache.get_object, fetch_and_cache)

    def get_object_stream(self, name: PurePosixPath | str) -> ObjectStream:
        def fetch_and_cache(n):
            with self._main.get_object_stream(n) as stream:
                self._cache.put_object_stream(n, stream)
            return self._cache.get_object_stream(n)

        return self._get_with_cache_fallback(name, self._cache.get_object_stream, fetch_and_cache)

    def put_object(self, name: PurePosixPath | str, content: Union[str, bytes, bytearray]) -> None:
        raise io.UnsupportedOperation("put_object is not supported for CachedImmutableMinioObjectStorage")

    def put_object_stream(self, name: PurePosixPath | str, stream: BinaryIO) -> None:
        raise io.UnsupportedOperation("put_object_stream is not supported for CachedImmutableMinioObjectStorage")

    def list_objects(self, prefix: PurePosixPath | str = "") -> slist[PurePosixPath]:
        return self._main.list_objects(prefix)

    def shallow_list_objects(self, prefix: PurePosixPath | str = "") -> ShallowListing:
        return self._main.shallow_list_objects(prefix)

    def exists(self, name: PurePosixPath | str) -> bool:
        return self._cache.exists(name) or self._main.exists(name)

    def remove_objects(self, names: Iterable[PurePosixPath | str]) -> slist[DeleteError]:
        raise io.UnsupportedOperation("remove_objects is not supported for CachedImmutableMinioObjectStorage")

    @classmethod
    def build_from_fs(cls, cache_root: Path, main: IBucket) -> "CachedImmutableBucket":
        cache_bucket = AppendOnlyFSBucket.build(cache_root)
        return CachedImmutableBucket(cache=cache_bucket, main=main)

    def get_size(self, name: PurePosixPath | str) -> int:
        try:
            return self._cache.get_size(name)
        except FileNotFoundError:
            return self._main.get_size(name)
