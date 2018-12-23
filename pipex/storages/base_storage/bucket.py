import time
import logging

from typing import Iterator, Tuple, Optional
from contextlib import contextmanager
from uuid import uuid4

from ..bucket_metadata import BucketMetadata
from ...pbase import Source, Sink, SourceDataVersion, SinkDataVersion, TransformedSource, Pipeline
from ...pdatastructures import PRecord


class Bucket(Source, Sink):
    def __init__(self, storage, *,
                 scope: Tuple[str],
                 use_batch: bool,
                 batch_size: Optional[int],
                 flush_interval: float):
        self.storage = storage
        self.scope = scope
        self.use_batch = use_batch
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.logger = logging.getLogger("{}({!r}, {!r})".format(self.__class__.__name__, self.storage, self.scope))
        self._last_flush_time = None

    def load_metadata(self, our) -> BucketMetadata:
        raise NotImplementedError

    def flush_metadata(self, our, metadata: BucketMetadata):
        raise NotImplementedError

    def load_ids(self, our) -> Iterator[str]:
        raise NotImplementedError

    def load_precord(self, our, id: str):
        raise NotImplementedError

    def save_precord(self, our, precord: PRecord):
        raise NotImplementedError

    def read_context(self):
        raise NotImplementedError

    def read_write_context(self):
        raise NotImplementedError

    def generate_precords(self, our) -> Iterator[PRecord]:
        with self.read_context():
            for id in self.load_ids(our):
                precord = self.load_precord(our, id)
                yield precord

    def fetch_source_data_version(self, our) -> SourceDataVersion:
        return self.load_metadata(our).fetch_source_data_version()

    def fetch_sink_data_version(self, our) -> SinkDataVersion:
        return self.load_metadata(our).fetch_sink_data_version()

    def process(self, our, tr_source: TransformedSource) -> Iterator[PRecord]:
        pipeline = tr_source.with_sink(self)
        if pipeline.rewriting_required(our):
            self.logger.info("Start (re)writing: {!r}".format(pipeline))
            return self.process_rewrite(our, pipeline, tr_source)
        else:
            self.logger.info("Upstream not modified. Skip writing.")
            return self.generate_precords(our)

    def process_rewrite(self,
                        our,
                        pipeline: Pipeline,
                        tr_source: TransformedSource) -> Iterator[PRecord]:
        with self.read_write_context():
            metadata = self.load_metadata(our)
            source_data_hash = pipeline.source.fetch_source_data_version(our).data_hash
            source_chain_hash = pipeline.transformer.chain_hash()
            try:
                if self.use_batch:
                    if self.batch_size is None:
                        # full batch
                        for precord in tr_source.execute(our):
                            self._save_precord_with_flush(our, precord, metadata)
                        yield from self.generate_precords(our)
                    else:
                        # mini batch
                        mini_batch = []
                        for index, precord in enumerate(tr_source.execute(our)):
                            self._save_precord_with_flush(our, precord, metadata)
                            mini_batch.append(precord)
                            if (index + 1) % self.batch_size:
                                yield from mini_batch
                                mini_batch.clear()
                        if mini_batch:
                            yield from mini_batch
                            mini_batch.clear()
                else:
                    # stream
                    for precord in tr_source.execute(our):
                        self._save_precord_with_flush(our, precord, metadata)
                        yield precord
            finally:
                last_source_data_hash = pipeline.source.fetch_source_data_version(our).data_hash
                metadata.source_data_hash = last_source_data_hash
                metadata.source_chain_hash = source_chain_hash
                metadata.data_hash = str(uuid4())
                self.flush_metadata(our, metadata)

    def _save_precord_with_flush(self, our, precord: PRecord, metadata: BucketMetadata):
        self.save_precord(our, precord)
        metadata.latest_record_timestamp = max(
            metadata.latest_record_timestamp,
            precord.timestamp,
        )
        now = time.time()
        if self._last_flush_time is None or now - self._last_flush_time > self.flush_interval:
            self.flush_metadata(our, metadata)
            self._last_flush_time = now
