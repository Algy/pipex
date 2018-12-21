import pytest
import numpy as np

from pipex import PStorage, channel_map

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

    
