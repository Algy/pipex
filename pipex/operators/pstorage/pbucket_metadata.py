from typing import Tuple, Optional, List
from functools import total_ordering

from ..pbase import SourceDataVersion, SinkDataVersion


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



class PBucketMetadata:
    def __init__(self, *,
                 meta_version: PBucketVersion,
                 data_hash: Optional[str],
                 source_chain_hash: Optional[str],
                 source_data_hash: Optional[str],
                 latest_record_timestamp: int):
        self.meta_version = meta_version
        self.source_chain_hash = source_chain_hash
        self.source_data_hash = source_data_hash
        self.data_hash = data_hash
        self.latest_record_timestamp = latest_record_timestamp

    def fetch_source_data_version(self) -> SourceDataVersion:
        return SourceDataVersion(data_hash=self.data_hash)

    def fetrch_sink_data_version(self) -> SinkDataVersion:
        return SinkDataVersion(
            source_data_hash=self.source_data_hash,
            source_chain_hash=self.source_chain_hash,
        )

    def to_json(self):
        return {
            'meta_version': str(self.meta_version),
            'source_chain_hash': self.source_chain_hash,
            'source_data_hash': self.source_data_hash,
            'data_hash': self.data_hash,
            'latest_record_timestamp': self.latest_record_timestamp,
        }

    @classmethod
    def from_json(cls, data):
        return cls(
            meta_version=PBucketVersion.parse(data['meta_version']),
            source_chain_hash=data['source_chain_hash'],
            source_data_hash=data['source_data_hash'],
            data_hash=data['data_hash'],
            latest_record_timestamp=data['latest_record_timestamp'],
        )


    @classmethod
    def initial(cls, meta_version: PBucketVersion):
        return cls(
            meta_version=meta_version,
            source_chain_hash=None,
            source_data_hash=None,
            data_hash=None,
            latest_record_timestamp=0,
        )

__all__ = (
    "PBucketVersion",
    "PBucketMetadata",
)
