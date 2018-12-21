import pytest
import os

import pipex as p
import pipex.ext.tesserocr as tesserocr

data_dir = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "data",
)


def remove_alpha(img):
    return img[:, :, :3]


def test_tesserocr():
    logo_files = [os.path.join(data_dir, "tesseract_logo.png")]
    text = next((logo_files >> p.image.open | p.map(remove_alpha) | tesserocr.read).values())
    assert "Tesseract OCR" == text
