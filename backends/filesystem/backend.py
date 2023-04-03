import os
import struct
from datetime import datetime, timedelta
from pathlib import Path
from pydantic import ConstrainedStr, ConstrainedDecimal
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Tuple, Type
from decimal import Decimal
from enum import Enum

from ..backend import Backend
from .timestream_file import TimeStreamFile
import pytz

if TYPE_CHECKING:
    from schema import TimeSeries

FMT_MAPPING = {
    int: "I",
    float: "d",
    str: "s",
}

FILESYSTEM_FILEPATH_FORMAT_DEFAULT = "{table}/{dimensions}/{year}/{month:02d}/{day:02d}"


class FileSystemBackend(Backend):
    root: Path
    endianess: str = "<"
    opened_files: Dict[str, "TimeStreamFile"]
    _struct_fmt: str = ""

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
        self._struct_fmt: str

    def persist(self, point: "TimeSeries") -> None:
        print(f"persisting {point.timestamp}")
        if not self._struct_fmt:
            self._struct_fmt = self.struct_fmt(cls=type(point))

        filename, bin_data = self.encode_point(point)

        timestream_file = self.timestream_file(filename)

        timestream_file.append(struct.pack(self._struct_fmt, *bin_data))

    def query(
        self,
        cls: Type["TimeSeries"],
        dimensions: Dict[str, Any],
        start_time: datetime,
        end_time: Optional[datetime] = None,
    ) -> Iterable["TimeSeries"]:
        lookup = TimeStreamFileLookup(cls, dimensions, start_time, end_time)

        self._struct_fmt = self.struct_fmt(cls=cls)
        files_queue = []

        for root, _, files in os.walk(self.root + "/" + cls.Meta.table):
            for file in files:
                filename = root + "/" + file
                if lookup.should_visit_file(filename):
                    files_queue.append(filename)

        files_queue.sort()
        for filename in files_queue:
            print("traversing", filename)
            timestream_file = TimeStreamFile(filename, self._struct_fmt)
            for binary_entry in timestream_file.entries(end_time):
                yield self.decode_point(cls, binary_entry)
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

    def struct_fmt(
        self,
        cls: Type["TimeSeries"] = None,
    ) -> str:
        # timestamp
        fmt = self.endianess + "I"

        if cls is not None:
            for _, info in cls.__fields__.items():
                field_info = info.field_info

                if info.name == "timestamp":
                    continue
                if issubclass(info.type_, ConstrainedStr):
                    fmt += f"{field_info.max_length}s"
                elif issubclass(info.type_, ConstrainedDecimal):
                    # let's encode a Decimal as int, where the value is
                    # multiplied by number of decimal places
                    # we can later optimize this by the number of max digits
                    fmt += "l"
                elif issubclass(info.type_, Enum):
                    fmt += "H"
                else:
                    fmt += FMT_MAPPING[info.type_]

        return fmt

    def decode_point(self, cls: Type["TimeSeries"], data: bytes) -> Dict[str, Any]:
        _data = struct.unpack(self._struct_fmt, data)
        _dict = {}
        for index, info in enumerate(cls.__fields__.values()):
            field_info = info.field_info

            if issubclass(info.type_, ConstrainedStr):
                _dict[info.name] = _data[index].decode("utf-8").replace("\x00", "")
            elif issubclass(info.type_, ConstrainedDecimal):
                # we encode the decimal in units therefore we need to divide it
                # to restore the original value
                multiplied_value = _data[index]
                number_of_decimal_places = field_info.decimal_places
                _dict[info.name] = multiplied_value / 10**number_of_decimal_places

            else:
                _dict[info.name] = _data[index]

        return cls(**_dict)

    def encode_point(self, point: "TimeSeries") -> Tuple[str, bytes]:
        bin_data = [int(point.timestamp.timestamp())]

        data = point.dict()
        data.pop("timestamp")

        for field, value in data.items():
            if isinstance(value, str):
                bin_data.append(value.encode("utf-8"))
            elif isinstance(value, Decimal):
                number_of_decimal_places = point.__class__.__fields__[
                    field
                ].field_info.decimal_places
                bin_data.append(int(value * pow(10, number_of_decimal_places)))
            elif isinstance(value, Enum):
                bin_data.append(value.value)
            else:
                bin_data.append(value)

        return (self.filename(point), bin_data)

    def timestream_file(self, path: str) -> "TimeStreamFile":
        if path not in self.opened_files:
            self.opened_files[path] = TimeStreamFile(path, self._struct_fmt)

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
