from functools import total_ordering
from typing import Tuple

@total_ordering
class BucketVersion:
    def __init__(self, positions: Tuple[int]):
        self.positions = positions

    def __str__(self):
        return ".".join(str(position) for position in self.positions)

    def __lt__(self, other):
        return self.positions < other.positions

    def __eq__(self, other):
        return self.positions == other.positions

    def __hash__(self):
        return hash(self.positions)

    @classmethod
    def parse(cls, ver: str):
        return cls(tuple(map(int, ver.split("."))))
