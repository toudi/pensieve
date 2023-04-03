import os
import struct
from dataclasses import dataclass
from datetime import datetime
from typing import BinaryIO, ClassVar, Dict, Iterable, List, NamedTuple, Optional

from .merge_file import mergeInPlace
from .cache import Cache


class TimeStreamFile:
    def __init__(self, file: str, fmt: str):
        self.file = self._open_file(file)
        self.fmt = fmt
        self.sizeof_struct = struct.calcsize(fmt)
        # how many entries are there in the file.
        # we can use it to our advantage when merging the sorted and unsorted part.
        self.file_entries: Optional[int] = None
        self.new_entries: List[bytes] = []
        self.cache = Cache(self.file, self.sizeof_struct)

    def __getitem__(self, index: int) -> int:
        return self.timestamp(self.cache[index])

    def __setitem__(self, index: int, content: bytes) -> None:
        self.cache[index] = content

    def __len__(self) -> int:
        if self.file_entries is None:
            self.file.seek(0, os.SEEK_END)
            self.file_entries = self.file.tell() // self.sizeof_struct
        return self.file_entries

    ## helper method
    def _open_file(self, filename: str) -> BinaryIO:
        open_mode = "r+b"

        if not os.path.exists(filename):
            open_mode = "w+b"

            _dirname = os.path.dirname(filename)
            if not os.path.exists(_dirname):
                os.makedirs(_dirname)

        return open(filename, open_mode)

    def timestamp(self, data: bytes) -> int:
        # timestamp is always the first value
        return struct.unpack(self.fmt, data)[0]

    def append(self, data: bytes) -> None:
        self.new_entries.append(data)

    def sort(self):
        new_entries = len(self.new_entries)

        mergeInPlace(
            self,
            num_new_items=new_entries,
            num_all_items=len(self) + new_entries,
            swap=self.cache.swap,
            progress=self.cache.sync,
        )

    def commit(self):
        print("commit")
        existing_items_count = len(self)
        # sort new items by timestamp
        self.new_entries.sort(key=self.timestamp)
        if existing_items_count > 0:
            # the file is going to be sorted therefore let's populate
            # the cache with new items.
            offset = existing_items_count
            for index, data in enumerate(self.new_entries):
                self.cache[offset + index] = data
        # write all new items to the file
        self.file.seek(0, os.SEEK_END)
        self.file.write(b"".join(self.new_entries))
        # sort the file (if needed) and close the underlying BinaryIO handle
        if existing_items_count > 0:
            self.sort()
        # persist unwritten changes
        self.cache.sync(existing_items_count + len(self.new_entries))
        print("dump file")
        self.dump()
        self.file.close()
        print("commit end")

    def dump(self):
        self.file_entries = None
        for i in range(len(self)):
            ts = datetime.fromtimestamp(self[i])
            print(f"timestamp[{i}] => {ts}")

    def entries(self, end_time: Optional[datetime] = None) -> Iterable[bytes]:
        offset = 0
        self.file.seek(0, os.SEEK_END)
        eof = self.file.tell()

        while offset < eof:
            self.file.seek(offset, os.SEEK_SET)
            data = self.file.read(self.sizeof_struct)
            timestamp = self.timestamp(data)

            if end_time is not None and timestamp > end_time.timestamp():
                break

            yield data
            offset += self.sizeof_struct

        self.file.close()
