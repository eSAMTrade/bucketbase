# isort: skip_file
# ruff: noqa

"""
beware of the order of imports, as some of the imports are circular, like fs_bucket due to (named_lock_manager)
"""
from bucketbase.ibucket import S3_NAME_CHARS_NO_SEP, IBucket, ShallowListing
from bucketbase.errors import DeleteError

from bucketbase.cached_immutable_bucket import CachedImmutableBucket
from bucketbase.file_lock import FileLockForPath
from bucketbase.fs_bucket import AppendOnlyFSBucket, FSBucket
from bucketbase.memory_bucket import MemoryBucket


def __getattr__(name: str) -> object:
    if name == "MinioBucket":
        try:
            from bucketbase.minio_bucket import MinioBucket
        except ModuleNotFoundError as exc:
            raise ImportError("MinioBucket requires the 'minio' extra: install bucketbase[minio]") from exc

        return MinioBucket
    if name in {"ObjectVersion", "VersionedMinioBucket"}:
        try:
            from bucketbase.versioned_minio_bucket import ObjectVersion, VersionedMinioBucket
        except ModuleNotFoundError as exc:
            raise ImportError("VersionedMinioBucket requires the 'minio' extra: install bucketbase[minio]") from exc

        return {"ObjectVersion": ObjectVersion, "VersionedMinioBucket": VersionedMinioBucket}[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
