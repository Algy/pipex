from ..poperators import source

from itertools import zip_longest

class merge(source):
    def __init__(self, *sources):
        self.sources = sources

    def generate_precords(self, our):
        for precords in zip_longest(*[source.generate_precords(our) for source in self.sources]):
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
