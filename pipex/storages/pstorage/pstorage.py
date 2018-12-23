import os
import numpy as np
import json
import time
import logging

from os.path import isfile, isdir, join
from typing import Optional

from .pbucket import PBucket
from ..base_storage import Storage

class PStorage(Storage):
    bucket_class = PBucket
    def __init__(self, base_dir='.'):
        self.base_dir = base_dir

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
