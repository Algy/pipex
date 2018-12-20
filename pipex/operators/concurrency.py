from ..poperators import transformer
from ..pbase import PipeChain

from functools import partial
from threading import Lock

from multiprocessing.pool import ThreadPool, Pool

#
# Fork-Join Model
#


def execute_on_worker(target, our, precord):
    # TODO
    p > target.execute(our)

class base_fork_join(pipe):
    def __init__(self, target: PipeChain, num_workers=None, shared_pool=False, unordered=True):
        self.target = target
        self.num_workers = num_workers
        self.shared_pool = shared_pool
        self.unordered = unordered

    def _get_pool(self):
        raise NotImplementedError

    def _close_pool(self, pool):
        pool.close()

    def transform(self, our, precords):
        fn = partial(execute_on_worker, self.target, our)
        pool = self._get_pool()
        try:
            if self.unordered:
                yield from pool.imap_unordered(precords, )
            else:
                yield from pool.imap( )
        finally:
            self._close_pool(pool)

class multithread(base_fork_join):
    shared_thread_pool = None
    lock = Lock()

    def _get_pool(self):
        cls = self.__class__
        if self.shared_pool:
            with cls.lock:
                if cls.shared_thread_pool is None:
                    cls.shared_thread_pool = ThreadPool(self.num_workers)
                return cls.shared_thread_pool
        else:
            return ThreadPool(self.num_workers)



class multiprocess(base_fork_join):
    shared_process_pool = None
    lock = Lock()

    def _get_pool(self):
        cls = self.__class__
        if self.shared_pool:
            with cls.lock:
                if cls.shared_thread_pool is None:
                    cls.shared_thread_pool = Pool(self.num_workers)
                return cls.shared_thread_pool
        else:
            return Pool(
            self.num_workers,




class base_background_worker(pipe):
    pass

class on_thread(base_background_worker):
    pass

class on_process(base_background_worker):
    pass
