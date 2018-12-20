from ..pdatastructures import PRecord
from ..poperators import pipe, pipe_map, sink

from itertools import islice

class done(sink):
    def save(self, value):
        pass


class tap(pipe_map):
    def __init__(self, fn):
        self.fn = fn

    def map(self, value):
        self.fn(value)
        return value


class channel(pipe):
    def __init__(self, channel_name):
        self.channel_name = channel_name

    def transform(self, precords):
        channel_name = self.channel_name
        for precord in precords:
            yield precord.with_channel(channel_name)


class dup(pipe):
    def __init__(self, *names: str):
        self.names = names

    def transform(self, precords):
        for precord in precords:
            value = precord.value
            yield precord.merge(*{
                name: value,
                for name in self.names
            })

class batch(pipe):
    def __init__(self, batch_size: int):
        self.batch_size = batch_size

    def transform(self, precords):
        it = iter(precords)
        while True:
            mini_batch = islice(precords, self.batch_size)
            if not mini_batch: break
            yield PRecord.from_object(mini_batch, 'precord_batch')


class unbatch(pipe):
    def transform(self, precords):
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
        except ValueError:
            self.arg_position = 0

        self._curried_fn = self._curried()


class map(base_curriable):
    def map(self, value):
        return self._curried_fn(value)

class filter(pipe_map):
    def filter(self, value):
        return self._curried_fn(value)

class slice(pipe):
    def __init__(self, *args):
        self.args = args

    def transform(self, precords):
        return islice(precords, *args)

class


p.map(fn, 1, ..., )
