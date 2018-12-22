import os
import numpy as np
import json
import time
import logging

from os.path import isfile, isdir, join
from typing import Optional

from .pbucket import PBucket

class PStorage:
    def __init__(self, base_dir='.'):
        self.base_dir = base_dir

    def bucket(self, name,
               use_batch: bool = False,
               batch_size: Optional[int] = None,
               flush_interval: float = 1.0,
              ) -> PBucket:
        return PBucket(
            storage=self,
            scope=tuple(name.lstrip("/").rstrip("/").split("/")),
            use_batch=use_batch,
            batch_size=batch_size,
            flush_interval=flush_interval,
        )

    def find(self, prefix):
        prefix = prefix.lstrip("/").rstrip("/")
        dir_name = join(self.base_dir, *prefix.split("/"))
        return [
            prefix + "/" + name
            for name in os.listdir(dir_name)
        ]


    def bucket_names(self):
        return [
            dir
            for dir in os.listdir(self.base_dir)
            if isdir(dir) and isfile(join(dir, "pbucket.json"))
        ]

    def __getitem__(self, name: str) -> PBucket:
        return self.bucket(name)
