
from .pdatastructures import PRecord
from .pbase import Source, Transformer, Sink, pipex_hash

from typing import Iterator, Any


class AutoChainHashMixin:
    def chain_hash(self):
        cls = self.__class__
        return pipex_hash(
            cls.__module__ + "." + cls.__name__,
            *[segment for pair in self.__dict__.items() for segment in pair]
        )


class BaseMeta(type):
    def chain_hash(cls) -> str:
        return cls().chain_hash()


class SourceMeta(BaseMeta, Source):
    def generate_precords(cls, our) -> Iterator[PRecord]:
        return cls().generate_precords(our)


class TransformerMeta(BaseMeta, Transformer):

    def transform(cls, our, precords: Iterator[PRecord]) -> Iterator[PRecord]:
        return cls().transform(our, precords)

class SinkMeta(BaseMeta, Sink):
    def process(cls, our, precords) -> Iterator[PRecord]:
        return cls().process(our, precords)


class source(Source, AutoChainHashMixin, metaclass=SourceMeta):
    channel_name = 'default'

    def generate(self) -> Iterator[Any]:
        raise NotImplementedError

    def generate_precords(self, our) -> Iterator[PRecord]:
        for object in self.generate():
            yield PRecord.from_object(object, self.channel_name)


class pipe(Transformer, AutoChainHashMixin, metaclass=TransformerMeta):
    def transform(self, our, precords: Iterator[PRecord]) -> Iterator[PRecord]:
        fn = self.map
        for precord in precords:
            new_value = fn(precord.value)

class pipe_map(pipe):
    def filter(self, value: Any) -> bool:
        return True

    def map(self, value: Any) -> Any:
        return value

    def transform(self, our, precords: Iterator[PRecord]) -> Iterator[PRecord]:
        fn = self.map
        for precord in precords:
            value = precord.value
            if not self.filter(value): continue
            new_value = fn(value)
            yield precord.with_value(new_value)


class sink(Sink, AutoChainHashMixin, metaclass=SinkMeta):
    def save(self, obj: Any):
        raise NotImplementedError

    def process(self, our, precords: Iterator[PRecord]) -> Iterator[PRecord]:
        for precord in precords:
            self.save(precord.value)
            yield precord

    def bound(self, other):
        self.process(other)
        return self
