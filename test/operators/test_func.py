import pytest
import numpy as np

from pipex.operators import (
    done,
    tap,
    channel,
    preload,
    dup,
    batch,
    select_channels,
    unbatch,
    map,
    filter,
    slice,
    grep,
    take,
    drop
)

from pipex.pdatastructures import PRecord

def test_done():
    ([1,2,3] >> done).do()

def test_tap_simple():
    counter = 0
    def func(source):
        nonlocal counter
        counter += source

    assert list(([1,2,3] >> tap(func)).values()) == [1, 2, 3]
    assert counter == 6


def test_tap_curry():
    counter = 0
    def func(lhs, source):
        nonlocal counter
        counter += lhs - source

    assert list(([1,2,3] >> tap(func, 0, ...)).values()) == [1, 2, 3]
    assert counter == -6


def test_map_simple():
    assert list(([1,2,3] >> map(lambda x: x + 1)).values()) == [2,3,4]

def test_map_curry():
    assert list(([1,2,3] >> map(lambda x, y: x - y, 0, ...)).values()) == [-1,-2,-3]

def test_filter_simple():
    assert list(([1,2,3] >> filter(lambda x: x % 2 == 0)).values()) == [2]

def test_filter_curry():
    assert list(([1,2,3] >> filter(lambda x, y: x % y == 0, 4, ...)).values()) == [1, 2]


def test_batch():
    batches = list(([1,2,3,4] >> batch(3)).values())
    assert batches[0][0].value == 1
    assert batches[0][1].value == 2
    assert batches[0][2].value == 3
    assert batches[1][0].value == 4


def test_select_channels():
    arr = [PRecord.from_object(1).merge(a=1, b=2, c=3)]
    precord = list(arr >> select_channels("a", "c"))[0]
    assert set(precord.channels) == set(["a", "c"])

