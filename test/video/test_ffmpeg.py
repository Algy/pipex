import os
import pytest
import numpy as np

from pipex import video
from pipex.operators import constant, map
from tempfile import NamedTemporaryFile

data_dir = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "data",
)

def test_all_frames():
    arr = []
    pl = [os.path.join(data_dir, "public_domain.mp4")] >> video.open | video.frames >> arr
    pl.do()

    assert len(arr) == 48
    assert all(isinstance(precord.value, np.ndarray) for precord in arr)
    assert all(precord.value.shape == (1080, 1920, 3) for precord in arr)
    assert all(precord.value.dtype == np.uint8 for precord in arr)
    assert all(precord.active_channel == 'image_frame' for precord in arr)

def test_frame_per_second():
    arr = []
    pl = [os.path.join(data_dir, "public_domain.mp4")] >> video.open | video.frames(fps='1') >> arr
    pl.do()

    assert len(arr) == 2
    assert all(isinstance(precord.value, np.ndarray) for precord in arr)
    assert all(precord.value.shape == (1080, 1920, 3) for precord in arr)
    assert all(precord.value.dtype == np.uint8 for precord in arr)
    assert all(precord.active_channel == 'image_frame' for precord in arr)
