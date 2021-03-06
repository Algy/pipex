from ..poperators import source
from ..pdatastructures import PRecord

from functools import reduce
from ..pbase import PipeChain

class merge(source):
    def __init__(self, *pipe_chains: PipeChain):
        self.pipe_chains = pipe_chains

    def generate_precords(self, our):
        its = [pipe_chain.execute(our) for pipe_chain in self.pipe_chains]
        for precords in zip(*its):
            result = precords[0]
            channel_atoms = result.channel_atoms.copy()

            for precord in precords[1:]:
                channel_atoms.update(precord.channel_atoms)
            yield PRecord(
                id=result.id,
                active_channel=result.active_channel,
                channel_atoms=channel_atoms,
            )

        # Exaust all iterators
        for it in its:
            for _ in it:
                pass



class concat(source):
    def __init__(self, *sources):
        self.sources = sources

    def generate_precords(self, our):
        for source in self.sources:
            yield from source.generate_precords(our)


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
