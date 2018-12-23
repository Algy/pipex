import os
import json
import time
import logging

from os.path import isfile, isdir, join
from typing import Optional, Tuple
from contextlib import contextmanager

from ..base_storage import Storage
from .h5bucket import H5Bucket

class H5Storage(Storage):
    bucket_class = H5Bucket

    def __init__(self, base_dir: str, swmr: bool = False):
        self.base_dir = base_dir
        self.swmr = swmr

    def bucket_names(self):
        return [
            name.partition(".")[0]
            for name in os.listdir(self.base_dir)
            if name.endswith(".h5")
        ]

    def __getitem__(self, name: str) -> H5Bucket:
        return self.bucket(name)

    @contextmanager
    def with_h5file(self, scope: Tuple[str]):
        import h5py
        os.makedirs(join(self.base_dir, *scope[:-1]), exist_ok=True)
        with h5py.File(os.path.join(self.base_dir, *scope) + ".h5", swmr=self.swmr) as fp:
            yield fp
