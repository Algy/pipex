import pytest
import psutil
import time

import threading

from hashlib import md5

from pipex.operators.funcs import map
from pipex.operators.concurrency import parallel, threaded

def test_threaded():
    arr = []
    ([1,2,3,4,5,6,7,8,9] >> threaded(map(lambda x: -x)) >> arr).do()
    # Note: order is not guaranteed!
    assert set(precord.value for precord in arr) == set([-1,-2,-3,-4,-5,-6,-7,-8,-9])


    time.sleep(0.1)
    assert len(list(threading.enumerate())) == 1

def test_parallel():
    arr = []
    ([1,2,3,4,5,6,7,8,9] >> parallel(map(lambda x: -x)) >> arr).do()
    # Note: order is not guaranteed!
    assert set(precord.value for precord in arr) == set([-1,-2,-3,-4,-5,-6,-7,-8,-9])

    time.sleep(0.1)
    current_process = psutil.Process()
    children = current_process.children(recursive=True)
    assert len(children) == 0
    assert len(list(threading.enumerate())) == 1

def generate():
    for i in range(10, -1, -1):
        if i % 1 == 0:
            time.sleep(0.01)
        yield i


def test_parallel_errornous_case():
    arr = []
    with pytest.raises(ZeroDivisionError):
        (generate() >> parallel(map(lambda x: 10 // x)) >> arr).do()
    time.sleep(0.1)
    current_process = psutil.Process()
    children = current_process.children(recursive=True)
    assert len(children) == 0
    assert len(list(threading.enumerate())) == 1

def test_md5():
    arr = [str(i) for i in range(10000)]
    results = []
    pipe = arr >> parallel(map(lambda x: md5(x.encode() * 10000).hexdigest())) >> results
    pipe.do()


def p(x):
    return md5(x.encode() * 10000).hexdigest()

def test_md5_pool():
    from multiprocessing import Pool
    arr = [str(i) for i in range(10000)]
    pool = Pool()
    results = list(pool.map(p, arr))
