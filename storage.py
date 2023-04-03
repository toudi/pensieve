from datetime import datetime
from os import environ
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Type

from backends.aws_timestream import TimeStreamBackend
from backends.backend import Backend
from backends.filesystem.backend import FileSystemBackend
from backends.print import PrintBackend
from backends.redis import RedisBackend

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
    prepared: Dict[Type["TimeSeries"], bool] = {}

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
        self.prepared = {}
        return self

    def add(self, data: "TimeSeries") -> None:
        data_type = type(data)
        if data_type not in self.prepared:
            self.backend.prepare_type(data_type)
            self.prepared[data_type] = True
        self.backend.persist(data)

    def query(
        self,
        cls: Type["TimeSeries"],
        dimensions: Dict[str, Any],
        start_time: datetime,
        end_time: Optional[datetime] = None,
    ) -> Iterable["TimeSeries"]:
        if cls not in self.prepared:
            self.backend.prepare_type(cls)
        yield from self.backend.query(cls, dimensions, start_time, end_time)

    def __exit__(self, *args, **kwargs):
        self.backend.commit()
