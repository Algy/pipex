import inspect

from .pdatastructures import PRecord
from .pbase import Source, Transformer, Sink, pipex_hash
from typing import Iterator, Any


def chain_hash(self):
    if getattr(self, 'pass_through', False):
        return ''
    cls = self.__class__
    return pipex_hash(
        cls.__module__ + "." + cls.__name__,
        *[
            segment
            for pair in sorted(self.__dict__.items(), key=lambda item: item[0])
            for segment in pair
        ]
    )

SOURCE_MEMBERS = set(name for name, _ in inspect.getmembers(Source) if not name.startswith("_"))
TRANSFORMER_MEMBERS = set(name for name, _ in inspect.getmembers(Transformer) if not name.startswith("_"))
SINK_MEMBERS = set(name for name, _ in inspect.getmembers(Sink) if not name.startswith("_"))

class BaseMeta(type):
    def __getattribute__(cls, name):
        if issubclass(cls, Source) and name in SOURCE_MEMBERS:
            return getattr(cls(), name)
        elif issubclass(cls, Transformer) and name in TRANSFORMER_MEMBERS:
            return getattr(cls(), name)
        elif issubclass(cls, Sink) and name in SINK_MEMBERS:
            return getattr(cls(), name)
        return super().__getattribute__(name)

class SourceMeta(BaseMeta, Source):
    def generate_precords(cls, our) -> Iterator[PRecord]:
        return cls().generate_precords(our)


class TransformerMeta(BaseMeta, Transformer):
    def transform(cls, our, precords: Iterator[PRecord]) -> Iterator[PRecord]:
        return cls().transform(our, precords)

class SinkMeta(BaseMeta, Sink):
    def process(cls, our, precords) -> Iterator[PRecord]:
        return cls().process(our, precords)


class source(Source, metaclass=SourceMeta):
    channel_name = 'default'

    def generate(self) -> Iterator[Any]:
        raise NotImplementedError

    def generate_precords(self, our) -> Iterator[PRecord]:
        for object in self.generate():
            yield PRecord.from_object(object, self.channel_name)

    def chain_hash(self):
        return chain_hash(self)

class pipe(Transformer, metaclass=TransformerMeta):
    def transform(self, our, precords: Iterator[PRecord]) -> Iterator[PRecord]:
        raise NotImplementedError

    def __repr__(self):
        cls = self.__class__
        if cls.__module__.startswith("pipex."):
            return cls.__module__.replace(".operators.funcs", "") + "." + cls.__name__
        else:
            return cls.__module__ + "." + cls.__name__

    def chain_hash(self):
        return chain_hash(self)


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


class sink(Sink, metaclass=SinkMeta):
    def save(self, obj: Any):
        raise NotImplementedError

    def process(self, our, tr_source) -> Iterator[PRecord]:
        for precord in tr_source.generate_precords(our):
            self.save(precord.value)
            yield precord

    def chain_hash(self):
        return chain_hash(self)
