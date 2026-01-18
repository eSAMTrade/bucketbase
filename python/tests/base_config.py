from abc import ABC
from os import getenv
from pathlib import Path

ROOT = Path(__file__).parent.absolute()


class LocalTestConfig(ABC):
    MINIO_PUBLIC_SERVER = getenv("MINIO_PUBLIC_SERVER", "play.min.io")
    MINIO_ACCESS_KEY = getenv("MINIO_ACCESS_KEY", "Q3AM3UQ867SPQQA43P2F")
    MINIO_SECRET_KEY = getenv("MINIO_SECRET_KEY", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG")
    MINIO_DEV_TESTS_BUCKET = getenv("MINIO_DEV_TESTS_BUCKET", "bucketbase-test")
