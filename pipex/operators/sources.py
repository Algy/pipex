from ..poperators import source

from itertools import zip_longest
from ..pbase import PipeChain

class merge(source):
    def __init__(self, *pipe_chains: PipeChain):
        self.pipe_chains = pipe_chains

    def generate_precords(self, our):
        for precords in zip_longest(*[pipe_chain.execute(our) for pipe_chain in self.pipe_chains]):
            result = precords[0]
            for precord in precords[1:]:
                result = result.merge(precord.channels)
            yield result


class concat(source):
    def __init__(self, *sources):
        self.sources = sources

    def generate_precords(self, our):
        for source in self.sources:
            yield from source


class cat(source):
    def __init__(self, iterable, channel_name='default'):
        self.iterable = iterable
        self.channel_name = channel_name

    def generate(self):
        yield from self.iterable


class repeat(source):
    def __init__(self, value=None):
        self.value = value

    def generate(self):
        value = self.value
        while True:
            yield value


__all__ = (
    'merge', 'concat', 'cat', 'repeat',
)
