import threading
from pathlib import Path

from bucketbase.file_lock import FileLockForPath


class NamedLockManager:
    """Abstract base class for managing locks by name"""
    def get_lock(self, name: str, only_existing=False) -> FileLockForPath:
        """Get a lock for the given name
        :param name: name of the object to lock for
        :param only_existing: If True, return only an existing lock if available; if false - create a new one if needed
        """
        raise NotImplementedError()


class ThreadLockManager(NamedLockManager):
    """Thread-based lock manager (i.e. only in the same process)"""

    def __init__(self):
        self._locks = {}
        self._lock_dict_lock = threading.Lock()

    def get_lock(self, name: str, only_existing=False) -> threading.Lock:
        with self._lock_dict_lock:
            if name not in self._locks:
                if only_existing:
                    raise RuntimeError(f"Object {name} is not locked")
                self._locks[name] = threading.Lock()
            return self._locks[name]


class FileLockManager(NamedLockManager):
    """File-based lock manager using FileLockForPath, for inter-process locking"""

    def __init__(self, lock_dir: Path):
        self._lock_dir = lock_dir
        self._lock_dir.mkdir(parents=True, exist_ok=True)
        self._locks = {}
        self._lock_dict_lock = threading.Lock()

    def get_lock(self, name: str, only_existing=False) -> FileLockForPath:
        with self._lock_dict_lock:
            if name not in self._locks:
                if only_existing:
                    raise RuntimeError(f"Object {name} is not locked")
                # Sanitize name to be a valid filename
                safe_name = name.replace('/', '#').replace('\\', '#')
                lock_path = self._lock_dir / safe_name
                self._locks[name] = FileLockForPath(lock_path)
            lock = self._locks[name]
            return lock

