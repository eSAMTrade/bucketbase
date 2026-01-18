class DeleteError:
    """Delete error information. This class is copied (and truncated) from Minio library"""

    def __init__(self, code: str, message: str, name: str) -> None:
        self._code = code
        self._message = message
        self._name = name

    @property
    def code(self) -> str:
        """Get error code."""
        return self._code

    @property
    def message(self) -> str:
        """Get error message."""
        return self._message

    @property
    def name(self) -> str:
        """Get name."""
        return self._name
