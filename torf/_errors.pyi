from _typeshed import StrPath

from ._utils import Filepaths

class TorfError(Exception):
    def __init__(self, msg: str, *posargs: object, **kwargs: object) -> None: ...

class URLError(TorfError):
    def __init__(self, url: str) -> None: ...
    @property
    def url(self) -> str: ...

class PieceSizeError(TorfError):
    def __init__(self, size: int, min: int | None = None, max: int | None = None) -> None: ...
    @property
    def size(self) -> int: ...
    @property
    def min(self) -> int | None: ...
    @property
    def max(self) -> int | None: ...

class MetainfoError(TorfError):
    def __init__(self, msg: str) -> None: ...

class BdecodeError(TorfError):
    def __init__(self, filepath: StrPath | None = None) -> None: ...
    @property
    def filepath(self) -> StrPath | None: ...

class MagnetError(TorfError):
    def __init__(self, uri: str, reason: str | None = None) -> None: ...
    @property
    def uri(self) -> str: ...
    @property
    def reason(self) -> str | None: ...

class PathError(TorfError):
    def __init__(self, path: StrPath, msg: str) -> None: ...
    @property
    def path(self) -> StrPath: ...

class CommonPathError(TorfError):
    def __init__(self, filepaths: Filepaths) -> None: ...
    @property
    def filepaths(self) -> Filepaths: ...

class VerifyIsDirectoryError(TorfError):
    def __init__(self, path: StrPath) -> None: ...
    @property
    def path(self) -> StrPath: ...

class VerifyNotDirectoryError(TorfError):
    def __init__(self, path: StrPath) -> None: ...
    @property
    def path(self) -> StrPath: ...

class VerifyFileSizeError(TorfError):
    def __init__(self, filepath: StrPath, actual_size: int | None, expected_size: int) -> None: ...
    @property
    def filepath(self) -> StrPath: ...
    @property
    def actual_size(self) -> int | None: ...
    @property
    def expected_size(self) -> int: ...

class VerifyContentError(TorfError):
    def __init__(
        self, filepath: StrPath, piece_index: int, piece_size: int, file_sizes: tuple[tuple[str, int], ...]
    ) -> None: ...
    @property
    def filepath(self) -> StrPath: ...
    @property
    def piece_index(self) -> int: ...
    @property
    def piece_size(self) -> int: ...
    @property
    def files(self) -> tuple[tuple[str, int], ...]: ...

class ReadError(TorfError):
    def __init__(self, errno: int, path: StrPath | None = None) -> None: ...
    @property
    def path(self) -> StrPath | None: ...
    @property
    def errno(self) -> int: ...

class MemoryError(TorfError, MemoryError): ...  # type: ignore[misc]

class WriteError(TorfError):
    def __init__(self, errno: int, path: StrPath | None = None) -> None: ...
    @property
    def path(self) -> StrPath | None: ...
    @property
    def errno(self) -> int: ...

class ConnectionError(TorfError):
    def __init__(self, url: str, msg: str | None = None) -> None: ...
    @property
    def url(self) -> str: ...