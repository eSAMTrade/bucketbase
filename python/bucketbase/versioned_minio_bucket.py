from dataclasses import dataclass
from pathlib import PurePosixPath

import minio
from minio.datatypes import Object
from minio.deleteobjects import DeleteError, DeleteObject
from pyxtension import validate
from streamerate import slist
from streamerate import stream as sstream
from urllib3 import BaseHTTPResponse

from bucketbase.ibucket import ObjectStream
from bucketbase.minio_bucket import MinioBucket, MinioObjectStream

try:
    from aicodesign import ai_draft
except ModuleNotFoundError:
    ai_draft = lambda _model: lambda obj: obj  # type: ignore[assignment]  # noqa: E731


@ai_draft("GPT-5")
@dataclass(frozen=True)
class ObjectVersion:
    name: PurePosixPath
    version_id: str
    is_latest: bool
    is_delete_marker: bool = False


@ai_draft("GPT-5")
class VersionedMinioBucket(MinioBucket):
    @staticmethod
    @ai_draft("GPT-5")
    def _to_bool(value: object) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        return str(value).lower() == "true"

    @classmethod
    @ai_draft("GPT-5")
    def _to_object_version(cls, obj: Object) -> ObjectVersion:
        object_name = cls._get_object_name(obj)
        version_id = obj.version_id
        if version_id is None:
            raise ValueError(f"Minio object listing item {object_name} has no version id")

        return ObjectVersion(
            name=PurePosixPath(object_name),
            version_id=version_id,
            is_latest=cls._to_bool(obj.is_latest),
            is_delete_marker=cls._to_bool(obj.is_delete_marker),
        )

    @ai_draft("GPT-5")
    def list_object_versions(self, name: PurePosixPath | str) -> slist[ObjectVersion]:
        _name = self._validate_name(name)
        listing_itr = self._minio_client.list_objects(bucket_name=self._bucket_name, prefix=_name, recursive=True, include_version=True)
        return sstream(listing_itr).filter(lambda obj: self._get_object_name(obj) == _name).map(self._to_object_version).to_list()

    @ai_draft("GPT-5")
    def get_object_version(self, name: PurePosixPath | str, version_id: str) -> bytes:
        with self.get_object_version_stream(name, version_id) as response:
            assert isinstance(response, BaseHTTPResponse), f"Expected IOBase, got {type(response)}"
            data = bytes()
            for buffer in response.stream(amt=1024 * 1024):
                data += buffer
            return data

    @ai_draft("GPT-5")
    def get_object_version_stream(self, name: PurePosixPath | str, version_id: str) -> ObjectStream:
        _name = self._validate_name(name)
        validate(isinstance(version_id, str), f"version_id must be str, but got {type(version_id)}", exc=ValueError)

        try:
            response: BaseHTTPResponse = self._minio_client.get_object(self._bucket_name, _name, version_id=version_id)
        except minio.error.S3Error as e:
            if e.code in ("MethodNotAllowed", "NoSuchKey", "NoSuchVersion"):
                raise FileNotFoundError(f"Object {_name} version {version_id} not found in bucket {self._bucket_name} on Minio") from e
            raise

        return MinioObjectStream(response, PurePosixPath(_name))

    @ai_draft("GPT-5")
    def remove_object_with_versions(self, name: PurePosixPath | str) -> slist[DeleteError]:
        versions = self.list_object_versions(name)
        if versions.size() == 0:
            return slist()

        delete_objects_stream = versions.map(lambda version: DeleteObject(str(version.name), version.version_id))
        return slist(self._minio_client.remove_objects(self._bucket_name, delete_objects_stream))
