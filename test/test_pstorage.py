import pytest
import numpy as np

from pipex import PStorage, channel_map, map, source

def test_pstorage():
    storage = PStorage("/tmp")
    image = np.array([[0, 0], [0, 0]], dtype=np.uint8)

    bucket = storage['pipex_test/test_pstorage']

    pl = [1, 2, 3] >> channel_map('image', lambda _: image) >> bucket
    pl.do()

    restored = list(bucket)

    assert len(restored) == 3
    for precord in restored:
        assert np.all(precord['image'] == image)
    assert set(precord.value for precord in restored) == set([1, 2, 3])


def test_pbucket_skipping(mocker):
    class TestSource(source):
        def __init__(self):
            self.counter = 0

        def generate(self):
            self.counter += 1
            yield from [1,2,3]

        def fetch_source_data_version(self, our):
            from pipex.pbase import SourceDataVersion
            return SourceDataVersion(data_hash="")

    storage = PStorage("/tmp")
    bucket = storage['pipex_test/test_pstorage']
    bucket_2 = storage['pipex_test/test_pstorage_2']

    test_source = TestSource()
    pl = test_source >> bucket >> map(lambda x: x + 1) >> bucket_2
    pl.do()
    assert test_source.counter == 1
    pl.do()
    # should skip
    assert test_source.counter == 1
