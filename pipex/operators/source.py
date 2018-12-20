from ..poperators import source

from itertools import zip_longest

class merge(source):
    def __init__(self, *sources):
        self.sources = sources

    def generate_precords(self):
        for precords in zip_longest(*[source.generate_precords() for source in self.sources]):
            result = precords[0]
            for precord in precords[1:]:
                result = result.merge(precord.channels)
            yield result

class concat(source):
    def __init__(self, *sources):
        self.sources = sources

    def generate_precords(self):
        for source in self.sources:
            yield from source

        
