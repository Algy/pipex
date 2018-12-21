import random

from ..pdatastructures import PRecord
from ..poperators import pipe, pipe_map, sink

from typing import Iterator

from itertools import islice

class done(sink):
    def save(self, value):
        pass

EMPTY = object()
class constant(pipe):
    def __init__(self, value=EMPTY, **channel_values):
        self.value = value
        self.channel_values = channel_values

    def transform(self, our, precords):
        for precord in precords:
            d = self.channel_values
            if self.value is not EMPTY:
                d = d.copy()
                d[precord.active_channel] = self.value
            yield precord.merge(**d)


class channel(pipe):
    def __init__(self, channel_name):
        self.channel_name = channel_name

    def transform(self, our, precords):
        channel_name = self.channel_name
        for precord in precords:
            yield precord.with_channel(channel_name)


class preload(pipe):
    def __init__(self, size=None):
        self.size = size

    def transform(self, our, precords):
        if self.size is None:
            yield from list(precords)
        else:
            while True:
                chunk = list(islice(precords, self.size))
                if not chunk:
                    break
                yield from chunk


class dup(pipe):
    def __init__(self, *names: str):
        self.names = names

    def transform(self, our, precords):
        for precord in precords:
            value = precord.value
            yield precord.merge(*{
                name: value
                for name in self.names
            })

class batch(pipe):
    def __init__(self, batch_size: int):
        self.batch_size = batch_size

    def transform(self, our, precords):
        while True:
            mini_batch = list(islice(precords, self.batch_size))
            if not mini_batch: break
            yield PRecord.from_object(mini_batch, 'precord_batch')


class unbatch(pipe):
    def transform(self, our, precords):
        for precord in precords:
            unbatched = precord.value
            yield from unbatched



class base_curriable(pipe_map):
    def __init__(self, fn, *args, **kwargs):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

        try:
           self.arg_position = list(args).index(...)
           self.args = tuple([x for x in args if x != ...])
        except ValueError:
            self.arg_position = 0
        self._curried_fn = self._curried()

    def _curried(self):
        if self.arg_position == 0:
            return self._simple_curry
        else:
            return self._insertion_curry

    def _simple_curry(self, x):
        return self.fn(x, *self.args, **self.kwargs)

    def _insertion_curry(self, x):
        my_args = list(self.args)
        my_args.insert(self.arg_position, x)
        return self.fn(*my_args, **self.kwargs)

class tap(base_curriable):
    def map(self, value):
        self._curried_fn(value)
        return value

class map(base_curriable):
    def map(self, value):
        return self._curried_fn(value)


class channel_map(base_curriable):
    def __init__(self, channel_name: str, fn, *args, **kwargs):
        super().__init__(fn, *args, **kwargs)
        self.channel_name = channel_name

    def transform(self, our, precords):
        for precord in precords:
            value = precord.value
            new_value = self._curried_fn(value)
            yield precord.merge(**{self.channel_name: new_value})


class filter(base_curriable):
    def filter(self, value):
        return self._curried_fn(value)

class slice(pipe):
    def __init__(self, *args):
        self.args = args

    def transform(self, our, precords):
        return islice(precords, *self.args)


class grep(pipe_map):
    def __init__(self, pattern=''):
        self.pattern = pattern

    def filter(self, value):
        return self.pattern in str(value)


class take(pipe):
    def __init__(self, n):
        self.n = n

    def transform(self, our, precords):
        return islice(precords, self.n)


class drop(pipe):
    def __init__(self, n):
        self.n = n

    def transform(self, our, precords):
        n = self.n
        for i, precord in enumerate(precords):
            if i < n:
                continue
            yield precord

class shuffle(pipe):
    def __init__(self, window_size=None):
        self.window_size = window_size

    def transform(self, our, precords):
        if self.window_size is None:
            window = list(precords)
            random.shuffle(window)
            yield from window
        else:
            while True:
                window = list(islice(precords, self.window_size))
                if not window:
                    break
                random.shuffle(window)
                yield from window

class select_channels(pipe):
    def __init__(self, *channels: str):
        self.channels = channels
        self._channel_set = set(channels)

    def transform(self, our, precords):
        channel_set = self._channel_set
        for precord in precords:
            yield precord.select_channels(channel_set)

__all__ = (
    'done', 'constant', 'tap', 'channel', 'dup', 'preload',
    'batch', 'unbatch', 'base_curriable',
    'map', 'channel_map', 'filter', 'slice', 'grep',
    'take', 'drop', 'shuffle', 'select_channels',
)
