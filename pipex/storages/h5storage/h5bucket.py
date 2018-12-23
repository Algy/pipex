import json
import numpy as np

from ..bucket_metadata import BucketMetadata
from ..bucket_version import BucketVersion

from ..base_storage import Bucket
from ...pdatastructures import PRecord, PAtom
from typing import Iterator
from contextlib import contextmanager


class H5Bucket(Bucket):
    META_VERSION = BucketVersion.parse('0.0.1')
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._h5file = None

    def load_metadata(self, our) -> BucketMetadata:
        with self._file_context():
            return self._ensure_metadata()

    def flush_metadata(self, our, metadata: BucketMetadata):
        with self._file_context():
            self._h5file.attrs['h5bucket.json'] = json.dumps(metadata.to_json())

    def load_ids(self, our) -> Iterator[str]:
        return self._h5file.keys()

    def load_precord(self, our, id: str):
        try:
            group = self._h5file[id]
        except KeyError:
            return None
        active_channel = group.attrs.get('active_channel', 'unknown')
        timestamp = group.attrs.get('timestamp')
        channel_atoms = {}
        for channel_name, dataset in group.items():
            format = dataset.attrs.get('format', 'unknown')
            data = np.array(dataset)
            if data.shape == ():
                data = data.tolist()
            patom = PAtom(value=data, format=format)
            channel_atoms[channel_name] = patom
        return PRecord(
            id=id,
            timestamp=timestamp,
            active_channel=active_channel,
            channel_atoms=channel_atoms,
        )

    def save_precord(self, our, precord: PRecord):
        try:
            group = self._h5file[precord.id]
        except KeyError:
            group = self._h5file.create_group(precord.id)
        group.attrs['active_channel'] = precord.active_channel
        group.attrs['timestamp'] = precord.timestamp
        for channel_name, patom in precord.channel_atoms.items():
            try:
                del group[channel_name]
            except KeyError:
                pass
            dataset = group.create_dataset(channel_name, data=patom.value)
            dataset.attrs['format'] = patom.format

    def read_context(self):
        return self._file_context()

    def read_write_context(self):
        return self._file_context()

    def _ensure_metadata(self):
        try:
            data_result = self._h5file.attrs['h5bucket.json']
            return BucketMetadata.from_json(json.loads(data_result))
        except KeyError:
            metadata = BucketMetadata.initial(self.META_VERSION)
            self._h5file.attrs['h5bucket.json'] = json.dumps(metadata.to_json())
            return metadata

    @contextmanager
    def _file_context(self):
        if self._h5file is None:
            try:
                with self.storage.with_h5file(self.scope) as h5file:
                    self._h5file = h5file
                    self._ensure_metadata()
                    yield
            finally:
                self._h5file = None
        else:
            yield
