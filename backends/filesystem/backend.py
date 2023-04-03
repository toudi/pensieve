import os
import struct
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Tuple, Type

import pytz
from pydantic import ConstrainedDecimal, ConstrainedStr

from ..backend import Backend
from .binary_struct import BinaryStruct
from .timestream_file import TimeStreamFile

if TYPE_CHECKING:
    from schema import TimeSeries


FILESYSTEM_FILEPATH_FORMAT_DEFAULT = "{table}/{dimensions}/{year}/{month:02d}/{day:02d}"


class FileSystemBackend(Backend):
    root: Path
    endianess: str = "<"
    opened_files: Dict[str, "TimeStreamFile"]
    structs: Dict[Type["TimeSeries"], "BinaryStruct"]

    def prepare_type(self, data_type: Type["TimeSeries"]) -> None:
        if data_type not in self.structs:
            self.structs[data_type] = BinaryStruct(data_type, self.endianess)

    def __init__(self):
        root = os.environ.get("TIME_SERIES_FS_ROOT", None)
        if root is None:
            raise ValueError("Please specify TIME_SERIES_FS_ROOT")
        self.filepath_format = os.environ.get(
            "TIME_SERIES_FS_FILEPATH_FORMAT", FILESYSTEM_FILEPATH_FORMAT_DEFAULT
        )

        self.root = root
        if not os.path.exists(root):
            os.makedirs(root)

        self.opened_files = {}
        self.structs = {}

    def persist(self, point: "TimeSeries") -> None:
        print(f"persisting {point.timestamp}")

        binary_struct = self.structs[type(point)]
        filename = self.filename(point)

        timestream_file = self.timestream_file(filename, binary_struct.fmt)
        timestream_file.append(binary_struct.encode_point(point))

    def query(
        self,
        cls: Type["TimeSeries"],
        dimensions: Dict[str, Any],
        start_time: datetime,
        end_time: Optional[datetime] = None,
    ) -> Iterable["TimeSeries"]:
        lookup = TimeStreamFileLookup(cls, dimensions, start_time, end_time)

        binary_struct = self.structs[cls]
        files_queue = []

        for root, _, files in os.walk(self.root + "/" + cls.Meta.table):
            for file in files:
                filename = root + "/" + file
                if lookup.should_visit_file(filename):
                    files_queue.append(filename)

        files_queue.sort()
        for filename in files_queue:
            print("traversing", filename)
            timestream_file = TimeStreamFile(filename, binary_struct.fmt)
            for binary_entry in timestream_file.entries(end_time):
                yield binary_struct.decode_point(binary_entry)
        return []

    def filename(self, point: "TimeSeries") -> Path:
        dimensions = []
        if "dimensions" in self.filepath_format:
            for dimension_name in point.Meta.dimensions:
                dimensions.append(dimension_name)
                dimensions.append(str(point.data.dimensions[dimension_name]))

        file_dir = Path(
            self.filepath_format.format(
                table=point.Meta.table,
                year=point.timestamp.year,
                month=point.timestamp.month,
                day=point.timestamp.day,
                dimensions="/".join(dimensions),
            )
        )
        return self.root / file_dir

    def timestream_file(self, path: str, struct_fmt: str) -> "TimeStreamFile":
        if path not in self.opened_files:
            self.opened_files[path] = TimeStreamFile(path, struct_fmt)

        return self.opened_files[path]

    def commit(self):
        for _, file_obj in self.opened_files.items():
            file_obj.commit()


class TimeStreamFileLookup:
    def __init__(
        self,
        cls: Type["TimeSeries"],
        dimensions: Dict[str, Any],
        start_time: datetime,
        end_time: Optional[datetime] = None,
    ):
        dimensions_path = []
        for dimension_name in cls.Meta.dimensions:
            dimensions_path.append(dimension_name)
            dimensions_path.append(str(dimensions[dimension_name]))

        self.dimensions_path_str = "/".join(dimensions_path)
        self.start_time = start_time
        self.end_time = end_time

    def should_visit_file(self, filename: str) -> bool:
        filename_parts = filename.split("/")
        date_parts = list(map(int, filename_parts[-3:]))
        # print(date_parts)
        file_date = datetime(*date_parts).replace(tzinfo=pytz.utc)
        should_visit = (
            self.dimensions_path_str in filename and file_date >= self.start_time
        )
        if self.end_time is not None:
            should_visit = should_visit and file_date < self.end_time + timedelta(
                days=1
            )

        return should_visit
