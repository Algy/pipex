import os
import json
import time
import logging

from os.path import isfile, isdir, join
from typing import Optional
from contextlib import contextmanager

from .h5bucket import H5Bucket

class H5Storage:
    def __init__(self, base_dir: str, swmr: bool = False):
        self.base_dir = base_dir
        self.swmr = swmr

    def bucket(self, name: str,
               use_batch: bool = True,
               batch_size: Optional[int] = None,
              ) -> H5Bucket:
        return PBucket(
            storage=self,
            scope=tuple(name.lstrip("/").rstrip("/").split("/")),
            use_batch=use_batch,
            batch_size=batch_size,
        )

    def bucket_names(self):
        return [
            name.partition(".")[0]
            for name in os.listdir(self.base_dir)
            if name.endswith(".h5")
        ]

    def __getitem__(self, name: str) -> H5Bucket:
        return self.bucket(name)

    @contextmanager
    def with_h5file(self, bucket_name):
        import h5py

        with h5py.File(os.path.join(self.base_dir, bucket_name) + ".h5", swmr=self.swmr) as fp:
            yield fp
