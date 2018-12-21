import logging

from ..poperators import pipe
from ..pbase import PipeChain, Source, Sink, Transformer

from threading import Thread
from queue import Queue as ThreadingQueue, Full, Empty
from multiprocessing import Process, cpu_count, Queue as ProcessingQueue, get_context
from typing import Optional, Type
from itertools import islice


class WorkerQuit(BaseException):
    pass

def safe_call(obj, mtdname):
    if hasattr(obj, mtdname):
        getattr(obj, mtdname)()

def close_queues(queues):
    for q in queues:
        safe_call(q, 'close')
    for q in queues:
        safe_call(q, 'cancel_join_thread')
    for q in queues:
        safe_call(q, 'oin_thread')


def _do_slice_chunk(precords, chunk_size):
    it = precords
    while True:
        chunk = list(islice(it, chunk_size))
        if not chunk:
            break
        yield chunk

def _slice_chunk(precords, chunk_size):
    return _do_slice_chunk(precords, chunk_size)


class ProducerThread(Thread):
    def __init__(self, chunk_size, poll_interval, in_q, precords, workers):
        super().__init__(daemon=True)
        self.chunk_size = chunk_size
        self.poll_interval = poll_interval
        self.in_q = in_q
        self.precords = precords
        self.raised_exception = None
        self.workers = workers
        self.asked_to_quit = False


    def run(self):
        logger = logging.getLogger("WorkerProducer")
        in_q, precords = self.in_q, self.precords
        try:
            for precord_chunk in _slice_chunk(precords, self.chunk_size):
                while True:
                    if self.asked_to_quit:
                        logger.debug("Someone asked me to quit quiting")
                        raise WorkerQuit
                    try:
                        logger.debug("Adding precord")
                        in_q.put(precord_chunk, timeout=self.poll_interval)
                        break
                    except Full:
                        pass
            logger.debug("All precords has been produced! Sending sentinels to workers")
            for _ in range(len(self.workers)):
                in_q.put(None)
            logger.debug("Sentinels produced!")
        except WorkerQuit:
            logger.debug("Worker Quit!")
        except Exception as exc:
            self.raised_exception = exc
            self.ask_quit(None)
        except KeyboardInterrupt:
            self.ask_quit("KeyboardInterrupt")

    def ask_quit(self, reason=None):
        self.asked_to_quit = True
        self._signal_workers_to_quit(reason)

    def _signal_workers_to_quit(self, reason):
        for worker in self.workers:
            worker.interrupt(reason)


class SourceFromProducerInWorker(Source):
    def __init__(self, owner: "Worker"):
        self.owner = owner

    def generate_precords(self, our):
        in_q = self.owner.in_q
        poll_interval = self.owner.poll_interval
        while True:
            self.owner._check_interrupt()
            try:
                precord_chunk = in_q.get(timeout=poll_interval)
                if precord_chunk is None: # met sentinel, which is the same as EOF. quit.
                    break
                yield from precord_chunk
            except Empty:
                pass


class Worker:
    def __init__(self, *,
                 chunk_size,
                 poll_interval,
                 ignore_error, error_logger,
                 name, our, target_chain,
                 in_q, out_q, ctl_in_q, ctl_out_q):
        self.chunk_size = chunk_size
        self.poll_interval = poll_interval
        self.ignore_error = ignore_error
        self.error_logger = error_logger
        self.name = name
        self.our = our
        self.target_chain = target_chain

        # in_q     -> Process/Thread(target=<Worker>) -> out_q
        # ctl_in_q ->                                 -> ctl_out_q (Control queue)
        self.in_q = in_q
        self.out_q = out_q

        # Just setting 'done' flag is not enough; processes don't share memory
        # We need another channel to control workers
        self.ctl_in_q = ctl_in_q
        self.ctl_out_q = ctl_out_q

        # Only meaningful in main thread
        self.is_done = False

    def run(self):
        self.logger = logger = logging.getLogger(self.name)
        logger.debug("Worker Started")
        try:
            for precord_chunk in _slice_chunk(self._generate(), self.chunk_size):
                while True:
                    self._check_interrupt()
                    try:
                        self.out_q.put(precord_chunk, timeout=self.poll_interval)
                        break
                    except Full:
                        pass

        except WorkerQuit:
            self._notify_parent_done()
        except Exception as exc:
            self.error_logger("Error raised in {}".format(self.name))
            self._ask_parent_raise(exc)
            raise
        finally:
            self._notify_parent_done()

    def _generate(self):
        target_chain = SourceFromProducerInWorker(self) >> self.target_chain
        for precord in target_chain.execute(self.our):
            yield precord

    def _ask_parent_raise(self, exc):
        if not self.ignore_error:
            print("PUT AERROR")
            self.ctl_out_q.put((False, exc))
            self.out_q.put(None)

    def _notify_parent_done(self):
        self.ctl_out_q.put((True, None))
        self.out_q.put(None)

    def _check_interrupt(self):
        try:
            self.ctl_in_q.get_nowait()
            raise WorkerQuit
        except Empty:
            pass

    # should be called by producer or main thread
    def interrupt(self, reason=None):
        self.ctl_in_q.put(reason or True)

    # should be called by main thread
    def pop_done_state(self):
        if self.is_done:
            return
        try:
            success, exc = self.ctl_out_q.get_nowait()
            self.is_done = True
            if not success:
                # Reraise
                raise exc
            return True
        except Empty:
            return False

# Fork-Join Model
class base_fork_join(pipe):
    queue_class = None # type: Optional[Type]
    process_class = None # type: Optional[Type]

    def __init__(self,
                 target_chain: PipeChain,
                 num_workers=None,
                 chunk_size=1,
                 queue_size=20,
                 ignore_error=False,
                 poll_interval=2.0,
                 error_logger=logging.error):
        if not isinstance(target_chain, (Sink, Transformer)):
            raise TypeError("{!r} not sink or transformer".format(target_chain))
        self.target_chain = target_chain
        self.num_workers = num_workers or cpu_count()
        self.chunk_size = chunk_size
        self.queue_size = queue_size
        self.ignore_error = ignore_error
        self.poll_interval = poll_interval
        self.error_logger = error_logger

    @property
    def _real_queue_size(self):
        # chunk_size + the number of sentinels
        return self.queue_size * self.num_workers + self.num_workers

    def _run_workers(self, our):
        in_q, out_q = self.queue_class(self._real_queue_size), self.queue_class(self._real_queue_size)
        workers = []
        for index in range(self.num_workers):
            name = self.get_worker_name(index)
            workers.append(
                Worker(
                    chunk_size=self.chunk_size,
                    poll_interval=self.poll_interval,
                    ignore_error=self.ignore_error,
                    error_logger=self.error_logger,
                    target_chain=self.target_chain,
                    name=name,
                    our=our,
                    in_q=in_q,
                    out_q=out_q,
                    ctl_in_q=self.queue_class(0),
                    ctl_out_q=self.queue_class(0),
                )
            )

        processes = []
        for worker in workers:
            process = self.process_class(
                target=worker.run,
                daemon=True,
            )
            process.start()
            processes.append(process)
        return workers, processes, in_q, out_q

    def _run_producer(self, precords, workers, in_q):
        producer = ProducerThread(
            self.chunk_size,
            self.poll_interval,
            in_q,
            precords,
            workers
        )
        producer.start()
        return producer

    def _run_consumer(self, workers, out_q):
        N = len(workers)
        num_done_workers = 0

        while num_done_workers < N or not out_q.empty():
            precord_chunk = out_q.get()
            if precord_chunk is not None:
                yield from precord_chunk
            else:
                # flush all queues
                num_done_workers += 1
                for worker in workers:
                    worker.pop_done_state()

        # Flush remaining objects inside the out queue
        try:
            while True:
                precord_chunk = out_q.get_nowait()
                if precord_chunk is not None:
                    yield from precord_chunk
        except Empty:
            pass

    def transform(self, our, precords):
        # [ producer thread ] => [ worker threads/processes ] => [ consumer(this thread) ]
        workers, processes, in_q, out_q = self._run_workers(our)
        producer = self._run_producer(precords, workers, in_q)
        try:
            yield from self._run_consumer(workers, out_q)
        finally:
            producer.ask_quit()

            # close all queues if possible
            queues = (
                [in_q, out_q] +
                [worker.ctl_in_q for worker in workers] +
                [worker.ctl_out_q for worker in workers]
            )
            close_queues(queues)

            # let's not make zombie processes.
            for proc in processes:
                proc.join()

        if producer.raised_exception is not None:
            raise producer.raised_exception

    def get_worker_name(self, index: int):
        return "Worker[{}]".format(index)


class threaded(base_fork_join):
    queue_class = ThreadingQueue
    process_class = Thread

    def get_worker_name(self, index: int):
        return "WorkerThread[{}]".format(index)


class parallel(base_fork_join):
    def __init__(self, *args, **kwargs):
        start_method = kwargs.pop('start_method', None) or 'spawn'
        super().__init__(*args, **kwargs)

        ctx = get_context(start_method)
        self.queue_class = ctx.Queue
        self.process_class = ctx.Process

    def get_worker_name(self, index: int):
        return "WorkerProcess[{}]".format(index)


class on_bg_thread(threaded):
    def __init__(self, *args, **kwargs):
        kwargs['num_workers'] = 1
        super().__init__(*args, **kwargs)

    def get_worker_name(self, index: int):
        return "BackgroundWorkerThread"


class on_bg_process(parallel):
    def __init__(self, *args, **kwargs):
        kwargs['num_workers'] = 1
        super().__init__(*args, **kwargs)

    def get_worker_name(self, index: int):
        return "BackgroundWorkerProcess"

__all__ = (
    'threaded', 'parallel',
    'on_bg_thread', 'on_bg_process',
)
