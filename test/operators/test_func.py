import pytest
import numpy as np

from pipex.operators import (
    done,
    tap,
    channel,
    preload,
    dup,
    batch,
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
