from .bucket import Bucket
from typing import Optional

class Storage:
    bucket_class = Bucket

    def bucket(self, name,
               use_batch: bool = True,
               batch_size: Optional[int] = None,
               flush_interval: float = 1.0,
              ) -> Bucket:
        return self.bucket_class(
            storage=self,
            scope=tuple(name.lstrip("/").rstrip("/").split("/")),
            use_batch=use_batch,
            batch_size=batch_size,
            flush_interval=flush_interval,
        )

    def __getitem__(self, name) -> Bucket:
        return self.bucket(name)
