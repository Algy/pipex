from ..bucket_metadata import BucketMetadata
from ..bucket_version import BucketVersion

class H5Bucket:
    META_VERSION = BucketVersion.parse('0.0.1')
    def __init__(self, *, storage, scope, use_batch, batch_size):
        self.storage = storage
        self.scope = scope
        self.use_batch = use_batch
        self.batch_size = batch_size
