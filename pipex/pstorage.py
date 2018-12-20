import os
import numpy as np
import json
import time
import logging

from os.path import splitext
from dateutil.parser import parse as parse_datetime
from datetime import datetime
from os.path import isfile, isdir, join
from typing import Tuple, Iterator, Optional, Any, Dict, List
from hashlib import sha1

from .pdatastructures import PAtom, PRecord
from .pbase import Source, Sink

class PStorage:
    SAVE_MODES = ('incremental', 'always')
    def __init__(self, base_dir='.'):
        self.base_dir = base_dir

    def bucket(self, name,
               use_batch: bool = False,
               batch_size: Optional[int] = None,
               flush_interval: float = 1.0,
               save_mode: str = 'incremental',
              ) -> "PBucket":
        return PBucket(
            storage=self,
            scope=(name, ),
            use_batch=use_batch,
            batch_size=batch_size,
            flush_interval=flush_interval,
            save_mode=save_mode,
        )

    def bucket_names(self):
        return [
            dir
            for dir in os.listdir(self.base_dir)
            if isdir(dir) and isfile(join(dir, "pbucket.json"))
        ]

    def __getitem__(self, name: str) -> "PBucket":
        return self.bucket(name)



from functools import total_ordering

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
                 source_chain_hash: Optional[str],
                 source_version: int,
                 latest_record_timestamp: int):
        self.meta_version = meta_version
        self.source_chain_hash = source_chain_hash
        self.source_version = source_version
        self.latest_record_timestamp = latest_record_timestamp


    def to_json(self):
        return {
            'version': self.version,
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



class PBucket(Source, Sink):
    META_VERSION = PBucketVersion.parse('0.0.1')

    def __init__(self, storage: PStorage,
                 scope: Tuple[str],
                 use_batch: bool,
                 batch_size: Optional[int],
                 flush_interval: float,
                 save_mode: str):
        self.storage = storage
        self.scope = scope
        self.use_batch = use_batch
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.save_mode = save_mode

        self.logger = logging.getLogger("PBucket({!r})".format(self.directory_name))
        self._last_flush_time = None

    @property
    def directory_name(self):
        return join(self.storage.base_dir, *self.scope)

    @property
    def meta_name(self):
        return join(self.directory_name, "pbucket.json")

    @property
    def meta_tmp_name(self):
        return join(self.directory_name, "pbucket.json.tmp")

    @property
    def data_directory_name(self):
        return join(self.directory_name, "data")

    # load
    def generate_precords(self, our) -> Iterator[PRecord]:
        self._ensure_pbucket_dir()
        directory_name = self.directory_name
        data_directory_name = self.data_directory_name

        for file_name in os.listdir(data_directory_name):
            id_str, file_ext = splitext(file_name)
            if file_ext != '.json':
                continue
            precord = self._load_precord(id_str)
            yield precord

    def _load_precord(self, id: str):
        data_directory_name = self.data_directory_name
        file_name = join(self.data_directory_name, id + ".json")
        with open(file_name) as f:
            d = json.load(f)
        id = d['id']
        default_channel = d['default_channel']
        channel_names = d['channel_names']
        channel_formats = d['channel_formats']
        timestamp = d['timestamp']
        data = d['data']
        channels = {}
        for channel_name, format in zip(channel_names, channel_formats):
            value = None
            if format == 'data':
                value = data.get(channel_name)
            else:
                channel_dir_name = open(join(directory_name, channel_name))
                if format == 'image':
                    ext = '.png'
                elif format == 'numpy.ndarray':
                    ext = '.npz'
                elif format == 'text':
                    ext = '.txt'
                else:
                    ext = '.dat'
                value_file_name = join(channel_dir_name, id + ext)

                if format == 'image':
                    with Image.open(value_file_name) as img:
                        value = np.array(img)
                elif format == 'numpy.ndarray':
                    value = np.load(value_file_name)
                elif format == 'text':
                    with open(value_file_name, "r") as f:
                        value = f.read()
                else:
                    with open(value_file_name, "rb") as f:
                        value = f.read()
            channels[name] = PAtom(value, format)
        return PRecord(
            id=id,
            channels=channels,
            timestamp=timestamp,
            default_channel=default_channel
        )

    def _save_precord(self, precord: PRecord):
        directory_name = self.directory_name
        data_directory_name = self.data_directory_name
        data_file_name = join(data_directory_name, precord.id + ".json")
        channel_names = list(precord.channels.keys())
        data = {}
        d = {
            "id": precord.id,
            "default_channel": precord.default_channel,
            "channel_names": channel_names,
            "channel_formats": [precord.channels[name].format for name in channel_names],
            "timestamp": precord.timestamp,
            "data": data,
        }

        id = precord.id
        for channel_name, patom in precord.channels.items():
            value, format = patom.value, patom.format
            if format == 'data':
                data[channel_name] = value
                continue
            elif format == 'image':
                ext = '.png'
            elif format == 'numpy.ndarray':
                ext = '.npz'
            elif format == 'text':
                ext = '.txt'
            else:
                ext = '.dat'

            channel_dir_name = open(join(directory_name, channel_name))
            value_file_name = join(channel_dir_name, id + ext)

            if format == 'image':
                Image.fromarray(value).save(value_file_name)
            elif format == 'numpy.ndarray':
                np.savez_compressed(value_file_name)
            elif format == 'text':
                with open(value_file_name, "w") as f:
                    f.write(value)
            else:
                with open(value_file_name, "wb") as f:
                    f.write(value)

    def _save_precord_with_flush(self, precord: PRecord, metadata: PBucketMetadata):
        self._save_precord(precord)
        metadata.latest_record_timestamp = max(
            metadata.latest_record_timestamp,
            precord.timestamp,
        )
        now = time.time()
        if self._last_flush_time is None or now - self._last_flush_time > self.flush_interval:
            self._flush_metadata(metadata)
            self._last_flush_time = now

    def process(self, our, tr_source: "TransformedSource") -> Iterator[PRecord]:
        self._ensure_pbucket_dir()
        metadata = self._load_metadata()

        use_existing = False
        timestamp
        if self.save_mode == 'incremental':
            source_chain_hash = tr_source.chain_hash()
            timestamp
            if source_chain_hash != metadata.source_chain_hash:
                pass




        try:
            if self.use_batch:
                if self.batch_size is None:
                    # full batch
                    for precord in tr_source.genenrate_precords():
                        self._save_precord_with_flush(precord, metadata)
                    yield from self.genenrate_precords()
                else:
                    # mini batch
                    mini_batch = []
                    for index, precord in enumerate(tr_source.genenrate_precords()):
                        self._save_precord_with_flush(precord, metadata)
                        mini_batch.append(precord)
                        if (index + 1) % self.batch_size:
                            yield from mini_batch
                            mini_batch.clear()
                    if mini_batch:
                        yield from mini_batch
                        mini_batch.clear()
            else:
                # stream
                for precord in tr_source.genenrate_precords():
                    self._save_precord_with_flush(precord, metadata)
                    yield precord
        finally:
            self._flush_metadata(metadata)


    def _load_metadata(self):
        meta_name = self.meta_name
        with open(meta_name) as f:
            data = json.load(f)
            return PBucketMetadata.from_json(data)

    def _flush_metadata(self, metadata: PBucketMetadata):
        meta_name = self.meta_data
        meta_tmp_name = self.meta_tmp_name

        if isfile(meta_tmp_name):
            raise RuntimeError("{} exists! Some other process might be modifying this bucket.".format(meta_tmp_name))

        with open(meta_tmp_name, "w") as f:
            f.write(json.dumps(metadata.to_json()))
        os.rename(meta_tmp_name, meta_name)

    def _ensure_pbucket_dir(self):
        directory_name = self.directory_name
        data_directory_name = self.data_directory_name
        meta_name = join(directory_name, "pbucket.json")

        self._ensure_dir(directory_name)
        self._ensure_dir(data_directory_name)

        if not isfile(meta_name):
            self._flush_metadata(PBucketMetadata.initial(self.META_VERSION))

    def _ensure_dir(self, name):
        if not isdir(name):
            os.makedirs(name)
