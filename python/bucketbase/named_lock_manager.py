import threading
from abc import ABC, abstractmethod
from pathlib import Path, PurePosixPath
from typing import Union

from bucketbase import IBucket
from bucketbase.file_lock import FileLockForPath

LockType = Union[threading.Lock, FileLockForPath]


class BaseNamedLockManager(ABC):
    """Abstract base class for managing locks by name"""

    @abstractmethod
    def get_lock(self, name: PurePosixPath | str, only_existing: bool = False) -> LockType:
        """Get a lock for the given name
        :param name: name of the object to lock for
        :param only_existing: If True, return only an existing lock if available; if false - create a new one if needed
        """
        raise NotImplementedError()


class ThreadLockManager(BaseNamedLockManager):
    """Thread-based lock manager (i.e. only in the same process)"""

    def __init__(self) -> None:
        self._locks: dict[str, LockType] = {}
        self._lock_dict_lock = threading.Lock()

    def get_lock(self, name: PurePosixPath | str, only_existing: bool = False) -> LockType:
        name = IBucket._validate_name(name)  # pylint: disable=protected-access
        with self._lock_dict_lock:
            if name not in self._locks:
                if only_existing:
                    raise RuntimeError(f"Object {name} is not locked")
                self._locks[name] = threading.Lock()
            return self._locks[name]


class FileLockManager(BaseNamedLockManager):
    """File-based lock manager using FileLockForPath, for inter-process locking"""

    LOCK_SEP = "#"

    def __init__(self, lock_dir: Path) -> None:
        self._lock_dir = lock_dir
        self._lock_dir.mkdir(parents=True, exist_ok=True)
        self._locks: dict[str, LockType] = {}
        self._lock_dict_lock = threading.Lock()

    def get_lock(self, name: PurePosixPath | str, only_existing: bool = False) -> LockType:
        name = IBucket._validate_name(name)  # pylint: disable=protected-access
        with self._lock_dict_lock:
            if name not in self._locks:
                if only_existing:
                    raise RuntimeError(f"Object {name} is not locked")
                # Sanitize name to be a valid filename
                safe_name = name.replace(IBucket.SEP, self.LOCK_SEP)
                lock_path = self._lock_dir / safe_name
                self._locks[name] = FileLockForPath(lock_path)
            lock = self._locks[name]
            return lock
