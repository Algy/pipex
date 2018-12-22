from typing import Tuple, Optional, List
from functools import total_ordering


class StorageVersionInfo:
    def get_source_chain_hash(self):
        raise NotImplementedError

    def get_source_version(self):
        raise NotImplementedError

    def get_latest_record_timestamp(self):
        raise NotImplementedError

@total_ordering
class PBucketVersion:
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



class PBucketMetadata(StorageVersionInfo):
    def __init__(self, *,
                 meta_version: PBucketVersion,
                 source_chain_hash: Optional[str],
                 source_version: int,
                 latest_record_timestamp: int):
        self.meta_version = meta_version
        self.source_chain_hash = source_chain_hash
        self.source_version = source_version
        self.latest_record_timestamp = latest_record_timestamp

    def get_source_chain_hash(self):
        return self.source_chain_hash

    def get_source_version(self):
        return self.source_version

    def get_latest_record_timestamp(self):
        return self.latest_record_timestamp

    def to_json(self):
        return {
            'meta_version': str(self.meta_version),
            'source_chain_hash': self.source_chain_hash,
            'source_version': self.source_version,
            'latest_record_timestamp': self.latest_record_timestamp,
        }

    @classmethod
    def from_json(cls, data):
        return cls(
            meta_version=PBucketVersion.parse(data['meta_version']),
            source_chain_hash=data['source_chain_hash'],
            source_version=data['source_version'],
            latest_record_timestamp=data['latest_record_timestamp'],
        )


    @classmethod
    def initial(cls, meta_version: PBucketVersion):
        return cls(
            meta_version=meta_version,
            source_chain_hash=None,
            source_version=1,
            latest_record_timestamp=0,
        )

__all__ = (
    "PBucketVersion",
    "PBucketMetadata",
    "StorageVersionInfo",
)
