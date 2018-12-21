import pytest
import numpy as np

from pipex.poperators import pipe_map
from pipex.pdatastructures import PRecord


class adder(pipe_map):
    def __init__(self, adding=1):
        self.adding = adding

    def map(self, value):
        return value + self.adding


def test_adder():
    assert list(([1, 2, 3] >> adder).values()) == [2, 3, 4]
    assert list(([1, 2, 3] >> adder(11)).values()) == [12, 13, 14]


    arr = []
    ([1, 2, 3] >> adder(11) >> arr).do()

    assert all(isinstance(x, PRecord) for x in arr)
    assert [precord.value for precord in arr] == [12, 13, 14]
