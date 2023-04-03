import os
from dataclasses import dataclass
from typing import BinaryIO, Dict


@dataclass
class CacheEntry:
    # file data at index
    data: bytes
    # this indicates whether the data was meant to be written
    # or just read
    modified: bool = False


class Cache:
    def __init__(self, file: BinaryIO, struct_size: int):
        self.entries: Dict[int, CacheEntry] = {}
        self.file = file
        self.struct_size = struct_size

    def __getitem__(self, index: int) -> bytes:
        print(f"get at index {index}")
        if index not in self.entries:
            self.file.seek(index * self.struct_size, os.SEEK_SET)
            self.entries[index] = CacheEntry(
                modified=False, data=self.file.read(self.struct_size)
            )
        return self.entries[index].data

    def __setitem__(self, index: int, content: bytes) -> None:
        print(f"set {index} => {content}")
        if index not in self.entries:
            self.entries[index] = CacheEntry(modified=True, data=content)
        else:
            self.entries[index].modified = True
            self.entries[index].data = content

    def swap(self, i: int, j: int) -> None:
        # swaps i'th position with j's position
        # please note that the algorithm merely gives us the index number, but
        # because we're sorting a file that has contents we need to read it
        # back by ourselves.
        print(f"swap {i} with {j}")
        if i == j:
            return

        self[i], self[j] = self[j], self[i]

    def sync(self, last_swap_index: int) -> None:
        # because this is a last known swap index, we can remove all entries from cache
        # that are based earlier than the last_swap_index as they won't be required
        # anymore
        # let's commit all the changes
        print(f"sync called with last_swap_index={last_swap_index}")
        for index, entry in self.entries.items():
            if entry.modified:
                self.file.seek(index * self.struct_size, os.SEEK_SET)
                print(f"write {entry.data} at {index*self.struct_size}")
                self.file.write(entry.data)
                entry.modified = False

        for index in range(last_swap_index):
            self.entries.pop(index, None)
