import numpy as np
import pytest
import os

from pipex import image
from pipex.operators import constant, map
from tempfile import NamedTemporaryFile

data_dir = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "data",
)


def test_open_image():
    results = []
    file_name = os.path.join(data_dir, '150px-Tux.svg.png')
    pipe = [file_name] >> image.open >> results
    pipe.do()
    assert isinstance(results[0].value, np.ndarray)
    assert results[0].get('file_name') == file_name


def test_save_image():
    from PIL import Image
    results = []
    with NamedTemporaryFile(suffix='.png') as file:
        pipe = [os.path.join(data_dir, '150px-Tux.svg.png')] >> image.open | map(lambda x: 255 - x) | constant(file_name=file.name) >> image.save
        pipe.do()
        buffer = file.read()
        assert buffer.startswith(b"\x89PNG")
