from .pdatastructures import PRecord
from functools import reduce
from typing import List, Iterator

def pipex_hash(type: str, *args: "PipeChain") -> str:
    text = ":".join(type, *[chain.chain_hash() for chain in args])
    return sha1(text.encode()).hexdigest().decode()


# NOTE: All descendants of PipeChain should be designed to be immutable
class PipeChain:
    def __lrshift__(self, lhs):
        # lhs(object that can be coerced to a Source) >> self
        return Source.coerce(lhs) >> self

    def chain_hash(self) -> str:
        raise NotImplementedError

    def __bool__(self):
        return True

    def execute(self, our) -> Iterator[PRecord]:
        raise NotImplementedError

    def do(self):
        for _ in self.execute({}):
            pass

class Source(PipeChain):
    @classmethod
    def coerce(cls, obj):
        if isinstance(obj, Source):
            return obj
        elif isinstance(obj, list):
            return ListSourceSink(obj)
        else:
            try:
                return IterSource(iter(obj))
            except TypeError:
                raise TypeError("Not a sink {!r}".format(obj))

    def __rshift__(self, other) -> PipeChain:
        if isinstance(other, Pipeline):
            # self >> (source >> (pipe1 | pipe2) >> sink)
            raise TypeError("Source cannot be attached to pipeline where source is already attached")
        elif isinstance(other, Transformer):
            # self >> (pipe1 | pipe2)
            return TransformedSource(self, other)
        else:
            other_as_sink = Sink.coerce(other)
            return Pipeline.direct_pipeline(self, other)

    def __iter__(self) -> Iterator[PRecord]:
        return self.generate_precords()

    def values(self) -> Iterator[Any]:
        for precord in self.generate_precords():
            yield precord.value

    def execute(self, our) -> Iterator[PRecord]:
        return self.generate_precords()

    def generate_precords(self) -> Iterator[PRecord]:
        raise NotImplementedError


class Transformer(PipeChain):
    def __rshift__(self, other):
        # transformer >> sink
        return TransformedSink(Sink.coerce(other))

    def __or__(self, other):
        # Transformer | Transformer (pipe operator)
        if isinstance(other, Transformer):
            return self.wrap_transformer(other)
        else:
            raise TypeError(
                "Transformer cannot pipe to {!r}. Did you forget to put parantheses around operators?".format(other)
            )

    def __ror__(self, lhs):
        raise TypeError("Transformer cannot be piped from {!r}, Did you forget to put parantheses around operators?.".format(lhs))

    def transform(self, precords: Iterator[PRecord]) -> Iterator[PRecord]:
        raise NotImplementedError

    def execute(self, our) -> Iterator[PRecord]:
        return iter(())

    def wrap_transformer(self, other: Transformer) -> Transformer:
        return TransformerSequence([self, other])


class TransformerSequence(Transformer):
    def __init__(self, transformers: List[Transformer] = None):
        self.transformers = transformers or []


    def transform(self, precords: Iterator[PRecord]) -> Iterator[PRecord]:
        return reduce(
            lambda prs, transformer: transformer.transform(prs),
            self.transformers,
            precords
        )

    def wrap_transformer(self, other: Transformer) -> Transformer:
        return TransformerSequence(self.transformers + [other])

    def __repr__(self):
        return " | ".join(repr(transformer) for transformer in self.transformers)



class Sink(PipeChain):
    @classmethod
    def coerce(cls, obj):
        if isinstance(obj, Sink):
            return obj
        elif isinstance(obj, list):
            return ListSourceSink(obj)
        elif obj is print:
            return PrintSink()
        else:
            raise TypeError("Not a sink {!r}".format(obj))

    def execute(self, our):
        return iter(())

    def process(self, our, tr_source: "TransformedSource") -> Iterator[PRecord]:
        raise NotImplementedError



class ListSourceSink(Source, Sink):
    def __init__(self, dest_list: List[PRecord]):
        self.dest_list = dest_list

    def get_hash(self) -> str:
        return str(id(self.dest_list))

    def generate_precords(self) -> Iterator[PRecord]:
        for item in self.dest_list:
            yield PRecord.from_object(item)

    def process(self, our, tr_source: "TransformedSource") -> Iterator[PRecord]:
        dest_list = self.dest_list
        for precord in tr_source.generate_precords():
            dest_list.append(precord)
            yield precord


class IterSource(Source):
    def __init__(self, it):
        self.it = it

    def generate_precords(self):
        for item in self.it:
            yield PRecord.from_object(item)

    def chain_hash(self) -> str:
        return pipex_hash("IterSource" + str(id(self.it)))


class PrintSink(Sink):
    def process(self, our, tr_source: "TransformedSource") -> Iterator[PRecord]:
        from pprint import pprint
        for precord in tr_source.generate_precords():
            pprint(precord.value)
            yield precord

    def chain_hash(self) -> str:
        return pipex_hash("PrintSink")


class IdentityTransformer(Transformer):
    _chain_hash = pipex_hash("IdentityTransformer")

    def transform(self, precords: Iterator[PRecord]) -> Iterator[PRecord]:
        return precords

    def chain_hash(self) -> str:
        return self._chain_hash


class TransformedSource(Source):
    def __init__(self, source: Source, transformer: Transformer):
        self.source = source
        self.transformer = transformer

    def chain_hash(self) -> str:
        return pipex_hash("TransformedSource", self.source, self.transformer)

    def with_sink(self, sink: Sink) -> "Pipeline":
        return Pipeline(self.source, self.transformer, sink)

    def generate_precords(self) -> Iterator[PRecord]:
        return self.transformer.transform(self.source.generate_precords())

    def __repr__(self):
        return "{!r} >> ({!r})".format(self.source, self.transformer)


class TransformedSink(Sink):
    def __init__(self, transformer: Transformer, sink: Sink):
        self.transformer = transformer
        self.sink = sink

    def chain_hash(self) -> str:
        return pipex_hash('TransformedSink', self.transformer, self.sink)

    def with_source(self, source: Source) -> "Pipeline":
        return Pipeline(TransformedSource(source, self.transformer), self.sink)

    def generate_precords(self) -> Iterator[PRecord]:
        return self.transformer.transform(self.source.generate_precords())

    def __repr__(self):
        return "({!r}) >> {!r}".format(self.transformer, self.sink)

    def execute(self, our):
        return it(())


class Pipeline(Source):
    def __init__(self, transformed_source, sink: Sink):
        self.transformed_source = transformed_source
        self.sink = sink

    @property
    def source(self):
        return self.transformed_source.source

    @property
    def tranformer(self):
        return self.transformed_source.transformer

    def chain_hash(self) -> str:
        return pipex_hash("Pipeline", self.transformed_source, self.sink)

    def generate_precords(self) -> Iterator[PRecord]:
        return self.transformed_source.generate_precords()

    def execute(self, our):
        return self.sink.process(our, self.transformed_source)

    @classmethod
    def direct_pipeline(cls, source: Source, sink: Sink):
        return cls(TransformedSource(source, IdentityTransformer()), sink)

    def __repr__(self):
        return "{!r} >> {!r}".format(self.transformed_source, self.sink)
