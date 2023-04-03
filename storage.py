from os import environ

from backends.aws_timestream import TimeStreamBackend
from backends.backend import Backend
from backends.filesystem.backend import FileSystemBackend
from backends.print import PrintBackend
from backends.redis import RedisBackend
from typing import TYPE_CHECKING, Iterable, Type, Dict, Any, Optional
from datetime import datetime

if TYPE_CHECKING:
    from .schema import TimeSeries

BACKENDS = {
    "fs": FileSystemBackend,
    "redis": RedisBackend,
    "print": PrintBackend,
    "timestream": TimeStreamBackend,
}


class Storage:
    backend: "Backend"

    # def __init__(self):
    #     print("init")
    #     pass

    def __enter__(self) -> "Storage":
        selected_backend = environ.get("TIME_SERIES_BACKEND")
        if selected_backend is None:
            raise ValueError(
                "No backend selected; Specify TIME_SERIES_BACKEND variable."
            )
        if selected_backend not in BACKENDS:
            raise ValueError("Invalid backend selected")
        self.backend = BACKENDS[selected_backend]()
        return self

    def add(self, data: "TimeSeries") -> None:
        self.backend.persist(data)

    def query(
        self,
        cls: Type["TimeSeries"],
        dimensions: Dict[str, Any],
        start_time: datetime,
        end_time: Optional[datetime] = None,
    ) -> Iterable["TimeSeries"]:
        yield from self.backend.query(cls, dimensions, start_time, end_time)

    def __exit__(self, *args, **kwargs):
        self.backend.commit()
