import pytest


import pipex as p


def test_merge():
    x = []
    (p.merge(
        p.cat([1,2,3,4], 'number'),
        p.cat(['a','b','c','d'], 'alphabet')
    ) >> x).do()

    for i, precord in enumerate(x):
        assert set(precord.channels) == {'number', 'alphabet'}
        assert precord.active_channel == 'number'
        assert precord['number'] == i + 1
        assert precord['alphabet'] == chr(ord('a') + i)
