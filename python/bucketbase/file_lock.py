import os
from pathlib import Path

import filelock


class FileLockForPath(filelock.FileLock):
    """
    Creates FileLock for a Path destination by creating a lock file with the same name extended with .lock
    """

    def __init__(self, path: Path, timeout: int = -1) -> None:
        lock_file_path = Path(str(path) + ".lock")
        self._lock_file_path = lock_file_path
        super().__init__(str(lock_file_path), timeout)

    def _acquire(self) -> None:
        self._lock_file_path.parent.mkdir(parents=True, exist_ok=True)
        return super()._acquire()

    def _release(self) -> None:
        try:
            super()._release()
        finally:
            # Remove the lock file after release on all platforms
            try:
                os.remove(self._lock_file_path)
            except OSError:
                pass
