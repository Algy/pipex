import pytest
import numpy as np

from pipex.pdatastructures import PRecord, PAtom

def test_precord_value():
    array = np.array([[1,2], [3, 4]], dtype=np.float32)
    precord = PRecord(id='1', timestamp=0.0)
    assert precord.value is None
    assert precord.active_channel == 'default'
    assert list(precord.channels) == []

    new_precord = precord.with_value(array)
    assert precord.value is None
    assert new_precord.value_format == 'numpy.ndarray'
    assert list(new_precord.channels) == ['default']
    assert np.all(new_precord.value == array)


def test_precord_merge():
    precord = PRecord(id='1', timestamp=0.0)
    precord = precord.with_value(1)
    new_precord = precord.merge(file_name='1.png')
    assert new_precord.value == 1
    assert new_precord.value_format == 'data'
    assert new_precord.with_channel('file_name').value == '1.png'
    assert new_precord.value == 1

def test_precord_blob():
    precord = PRecord(id='1', timestamp=0.0)
    precord = precord.with_value(b'somedata')
    assert precord.value_format == 'blob'

def test_precord_from_object():
    obj = object()
    precord_1 = PRecord.from_object(obj)
    assert precord_1.id == str(id(obj))
    assert precord_1.active_channel == 'default'

    obj = object()
    precord_1 = PRecord.from_object(obj, 'file_name')
    assert precord_1.id == str(id(obj))
    assert precord_1.active_channel == 'file_name'
