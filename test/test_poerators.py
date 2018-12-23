import pytest
import numpy as np

from pipex.poperators import source, pipe_map, pipe, sink, value_sink
from pipex.pdatastructures import PRecord

class one_forever(source):
    def generate(self):
        while True:
            yield 1


class set_sink(value_sink):
    def __init__(self, set):
        self.set = set

    def save_value(self, obj):
        self.set.add(obj)

class one_shot_sink(sink):
    def __init__(self):
        self.saved_value = None

    def process(self, our, precords):
        for precord in precords:
            self.saved_value = precord.value
            break
        yield from []

class adder(pipe_map):
    def __init__(self, adding=1):
        self.adding = adding

    def map(self, value):
        return value + self.adding

class select_even(pipe_map):
    def filter(self, value):
        return value % 2 == 0

class change_channel(pipe):
    def transform(self, our, precords):
        for precord in precords:
            yield precord.merge(another_channel=precord.value).with_channel('another_channel')

def test_adder():
    assert list(([1, 2, 3] >> adder).values()) == [2, 3, 4]
    assert list(([1, 2, 3] >> adder(11)).values()) == [12, 13, 14]


    arr = []
    ([1, 2, 3] >> adder(11) >> arr).do()
    assert all(isinstance(x, PRecord) for x in arr)
    assert [precord.value for precord in arr] == [12, 13, 14]

def test_filter():
    assert list(([1, 2, 3] >> select_even).values()) == [2]


def test_change_channel():
    for precord in ([1, 2, 3] >> change_channel):
        assert isinstance(precord, PRecord)
        assert precord.value in [1, 2, 3]
        assert precord.active_channel == 'another_channel'


def test_one_forever():
    s = one_shot_sink()
    (one_forever >> adder >> s).do()

    assert s.saved_value == 2

def test_set_sink():
    my_set = set()
    s = set_sink(my_set)
    ([1] >> adder >> s).do()
    assert my_set == set([2])
