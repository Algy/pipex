
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
from PIL import Image

from ..pdatastructures import PAtom, PRecord
from ..pbase import Source, Sink, SourceDataVersion, SinkDataVersion
from functools import total_ordering

from .pbucket_metadata import PBucketMetadata, PBucketVersion


class PBucket(Source, Sink):
    META_VERSION = PBucketVersion.parse('0.0.1')

    def __init__(self, storage,
                 scope: Tuple[str],
                 use_batch: bool,
                 batch_size: Optional[int],
                 flush_interval: float):
        self.storage = storage
        self.scope = scope
        self.use_batch = use_batch
        self.batch_size = batch_size
        self.flush_interval = flush_interval

        self.logger = logging.getLogger("PBucket({!r})".format(self.directory_name))
        self._last_flush_time = None

        self._dir_check_cache = {}

    @property
    def directory_name(self):
        return join(self.storage.base_dir, *self.scope)

    @property
    def meta_name(self):
        return join(self.directory_name, "pbucket.json")

    @property
    def meta_tmp_name(self):
        return join(self.directory_name, "pbucket.json.tmp")

    def get_sub_dir(self, name):
        return join(self.directory_name, 'pbkt_' + name)

    def ensure_sub_dir(self, name):
        try:
            return self._dir_check_cache[name]
        except KeyError:
            dir_name = self.get_sub_dir(name)
            os.makedirs(dir_name, exist_ok=True)
            self._dir_check_cache[name] = dir_name
            return dir_name

    @property
    def data_directory_name(self):
        return self.get_sub_dir('data')

    # load
    def generate_precords(self, our) -> Iterator[PRecord]:
        self._dir_check_cache = {}
        self._ensure_pbucket_dir()
        directory_name, data_directory_name = self.directory_name, self.data_directory_name


        for file_name in os.listdir(data_directory_name):
            id_str, file_ext = splitext(file_name)
            if file_ext != '.json':
                continue
            precord = self._load_precord(id_str)
            yield precord

    def _load_precord(self, id: str):
        data_directory_name = self.data_directory_name
        file_name = join(data_directory_name, id + ".json")
        with open(file_name) as f:
            d = json.load(f)
        id = d['id']
        active_channel = d['active_channel']
        channel_names = d['channel_names']
        channel_formats = d['channel_formats']
        timestamp = d['timestamp']
        data = d['data']
        channel_atoms = {}
        for channel_name, format in zip(channel_names, channel_formats):
            value = None
            if format == 'data':
                value = data.get(channel_name)
            else:
                channel_dir_name = self.ensure_sub_dir(channel_name)
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
            channel_atoms[channel_name] = PAtom(value=value, format=format)
        return PRecord(
            id=id,
            channel_atoms=channel_atoms,
            timestamp=timestamp,
            active_channel=active_channel,
        )

    def _save_precord(self, precord: PRecord):
        directory_name, data_directory_name = self.directory_name, self.data_directory_name
        data_file_name = join(data_directory_name, precord.id + ".json")
        channel_names = list(precord.channels)
        data = {}
        d = {
            "id": precord.id,
            "active_channel": precord.active_channel,
            "channel_names": channel_names,
            "channel_formats": [precord.get_atom(name).format for name in channel_names],
            "timestamp": precord.timestamp,
            "data": data,
        }

        id = precord.id
        for channel_name in precord.channels:
            patom = precord.get_atom(channel_name)
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

            channel_dir_name = self.ensure_sub_dir(channel_name)
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

        file_name = join(self.data_directory_name, id + ".json")
        with open(file_name, "w") as f:
            json.dump(d, f)

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

    def _save(self, tr_source):
        if hasattr(tr_source.source, 'get_storage_version_info'):
            source_version_info = tr_source.source.get_storage_version_info()
            my_version_info = self.get_source_version_info()

            if source_version_info.get_source_chain_hash() == tr_source.transformer.chain_hash():
                # Transformer has changed
                return 'all'
            elif source_version_info.get_source_version() > my_version_info.get_source_version():
                # Version of storage has increased
                return 'all'
            elif source_version_info.get_latest_record_timestamp() > my_version_info.get_latest_record_timestamp():
                ...

        else:
            return 'all'

    def process(self, our, tr_source: "TransformedSource") -> Iterator[PRecord]:
        self._dir_check_cache = {}
        self._ensure_pbucket_dir()
        metadata = self._load_metadata()

        try:
            if self.use_batch:
                if self.batch_size is None:
                    # full batch
                    for precord in tr_source.generate_precords(our):
                        self._save_precord_with_flush(precord, metadata)
                    yield from self.generate_precords(our)
                else:
                    # mini batch
                    mini_batch = []
                    for index, precord in enumerate(tr_source.generate_precords(our)):
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
                for precord in tr_source.generate_precords(our):
                    self._save_precord_with_flush(precord, metadata)
                    yield precord
        finally:
            self._flush_metadata(metadata)

    def fetch_source_data_version(self) -> SourceDataVersion:
        return self._load_metadata().fetch_source_data_version()

    def fetch_sink_data_version(self) -> SinkDataVersion:
        return self._load_metadata().fetch_sink_data_version()

    def _load_metadata(self):
        meta_name = self.meta_name
        with open(meta_name) as f:
            data = json.load(f)
            return PBucketMetadata.from_json(data)

    def _flush_metadata(self, metadata: PBucketMetadata):
        meta_name = self.meta_name
        meta_tmp_name = self.meta_tmp_name

        if isfile(meta_tmp_name):
            raise RuntimeError(
                "{} exists!".format(meta_tmp_name) +
                "Some other process might be modifying this bucket. " +
                "If you are sure it is not, remove the file and try again."
            )

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
            os.makedirs(name, exist_ok=True)
