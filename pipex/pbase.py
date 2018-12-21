from .pdatastructures import PRecord
from functools import reduce
from typing import List, Iterator, Any, cast, Union

from hashlib import sha1

def _ensure_hash(obj: Any):
    if hasattr(obj, "chain_hash"):
        return obj.chain_hash()
    else:
        return str(obj)

def pipex_hash(type: str, *args: "PipeChain") -> str:
    text = "".join([type, *[_ensure_hash(arg) for arg in args]])
    return sha1(text.encode()).hexdigest()


#
# As rshift(>>) operator has  higher precedence over pipe operator(|),
# Some hacks are employed.
# Case 1. Source >> pipe1 | pipe2
#   TransformedSource | pipe2
#   => Source >> pipe1 | pipe2
# Case 2. Source >> pipe1 | pipe2 >> Sink
#   It is grouped as (Source >> pipe1) | (pipe2 >> Sink)
#   TransformedSource | TransformedSink as sink
#   => Source >> pipe1 | pipe2 >> Sink
# Case 3. pipe1 | (pipe2 >> Sink)
#   pipe | TransformedSink


class We:
    @classmethod
    def default_value(cls) -> "We":
        return cls()


# NOTE: All descendants of PipeChain should be designed to be immutable
class PipeChain:
    def __lrshift__(self, lhs):
        # lhs(object that can be coerced to a Source) >> self
        return Source.coerce_source(lhs) >> self

    def __rshift__(self, other):
        chains = []
        coerced = Sink.coerce_sink(other)
        if coerced != NotImplemented:
            right_chain = coerced
        else:
            right_chain = other
        self.flatten_chains(chains)
        chains.append(">>")
        right_chain.flatten_chains(chains)
        return self.parse_chain(chains)

    def __or__(self, other):
        chains = []
        self.flatten_chains(chains)
        chains.append("|")
        other.flatten_chains(chains)
        return self.parse_chain(chains)

    def chain_hash(self) -> str:
        raise NotImplementedError

    def __bool__(self):
        return True

    def execute(self, our: We) -> Iterator[PRecord]:
        raise NotImplementedError

    def do(self):
        for _ in self.execute({}):
            pass

    def flatten_chains(self, results: List[Union["PipeChain", str]]):
        results.append(self)

    @classmethod
    def parse_chain(cls, chains: List[Union["PipeChain", str]]) -> "PipeChain":
        if len(chains) == 0:
            raise ValueError
        elif len(chains) == 1:
            if isinstnace(chains[0], str):
                raise ValueError
            return cast(PipeChain, chains[0])
        else:
            return cls._p_E(chains)

    @classmethod
    def _p_E(cls, chains):
        '''
        E ::= S ('>>' S)*
        S ::= P ('|' S)*
        P ::= Source | Transformer | Sink
        '''

        # redirecting segments
        redir_chains = []

        current_segment = []
        for c in chains:
            if c == '>>':
                redir_chains.append(cls._p_S(current_segment))
                current_segment.clear()
            else:
                current_segment.append(c)

        if current_segment:
            redir_chains.append(cls._p_S(current_segment))
            current_segment.clear()

        if any(c == NotImplemented for c in redir_chains):
            return NotImplemented
        return cls._wrap_redirection(redir_chains)


    @classmethod
    def _p_S(cls, chains) -> "PipeChain":
        # pipe segments
        chains = [c for c in chains if isinstance(c, PipeChain)]
        return cls._wrap_pipe(chains)

    @classmethod
    def _wrap_redirection(cls, chains):
        if not chains:
            return NotImplemented
        iter_chain = chains[0]
        for chain in chains[1:]:
            if isinstance(chain, Source):
                if not isinstance(chain, Sink):
                    return NotImplemented
                # assert isinstance(chain, Source) and isinstance(chain, Sink)
                if isinstance(iter_chain, TransformedSource):
                    iter_chain = iter_chain.with_sink(chain)
                elif isinstance(iter_chain, Source):
                    iter_chain = Pipeline.direct_pipeline(iter_chain, chain)
                elif isinstance(iter_chain, Transformer):
                    iter_chain = TransformedSink(iter_chain, chain)
                else:
                    return NotImplemented
            elif isinstance(chain, Transformer):
                if isinstance(iter_chain, Source):
                    iter_chain = TransformedSource(iter_chain, chain)
                else:
                    return NotImplemented
            elif isinstance(chain, Sink):
                if chain != chains[-1]:
                    return NotImplemented
                if isinstance(iter_chain, TransformedSource):
                    iter_chain = iter_chain.with_sink(chain)
                elif isinstance(iter_chain, Source):
                    iter_chain = Pipeline(
                        TransformedSource(iter_chain, IdentityTransformer()),
                        chain,
                    )
                elif isinstance(iter_chain, Transformer):
                    iter_chain = TransformedSink(iter_chain, chain)
                else:
                    return NotImplemented
            else:
                return NotImplemented
        return iter_chain

    @classmethod
    def _wrap_pipe(cls, chains):
        if len(chains) == 0:
            return NotImplemented
        elif len(chains) == 1:
            return chains[0]
        else:
            if not all(isinstance(chain, Transformer) for chain in chains):
                return NotImplemented
            return TransformerSequence(chains)






class Source(PipeChain):
    @classmethod
    def coerce_source(cls, obj) -> "Source":
        if isinstance(obj, Source):
            return obj
        elif isinstance(obj, list):
            return ListSourceSink(obj)
        else:
            try:
                return IterSource(iter(obj))
            except TypeError:
                raise TypeError("Not a sink {!r}".format(obj))

    def __iter__(self) -> Iterator[PRecord]:
        return self.generate_precords(We.default_value())

    def values(self, we = None) -> Iterator[Any]:
        for precord in self.generate_precords(we or We.default_value()):
            yield precord.value

    def execute(self, our: We) -> Iterator[PRecord]:
        return self.generate_precords(our)

    def generate_precords(self, our: We) -> Iterator[PRecord]:
        raise NotImplementedError


class Transformer(PipeChain):
    def transform(self, our: We, precords: Iterator[PRecord]) -> Iterator[PRecord]:
        raise NotImplementedError

    def execute(self, our: We) -> Iterator[PRecord]:
        return iter(())


    def wrap_transformer(self, other: "Transformer") -> "Transformer":
        self_is_seq, other_is_seq = isinstance(self, TransformerSequence), isinstance(other, TransformerSequence)
        if isinstance(other, BufferedTransformer):
            return BufferedTransformer(
                self.wrap_transformer(other.lhs_tr),
                other.sink,
                other.rhs_tr,
            )
        elif self_is_seq and other_is_seq:
            return TransformerSequence(
                cast(TransformerSequence, self).transformers +
                cast(TransformerSequence, other).transformers
            )
        elif self_is_seq and not other_is_seq:
            return TransformerSequence(cast(TransformerSequence, self).transformers + [other])
        elif not self_is_seq and other_is_seq:
            return TransformerSequence([self] + cast(TransformerSequence, other).transformers)
        else:
            return TransformerSequence([self, other])


class TransformerSequence(Transformer):
    def __init__(self, transformers: List[Transformer] = None):
        self.transformers = transformers or []

    def transform(self, our: We, precords: Iterator[PRecord]) -> Iterator[PRecord]:
        return reduce(
            lambda prs, transformer: transformer.transform(our, prs),
            self.transformers,
            precords
        )

    def flatten_chains(self, results: List[Union["PipeChain", str]]):
        for i, tr in enumerate(self.transformers):
            if i > 0:
                results.append("|")
            tr.flatten_chains(results)

    def __repr__(self):
        return " | ".join(repr(transformer) for transformer in self.transformers)



class Sink(PipeChain):
    @classmethod
    def coerce_sink(cls, obj) -> "Sink":
        if isinstance(obj, Sink):
            return obj
        elif isinstance(obj, list):
            return ListSourceSink(obj)
        elif obj is print:
            return PrintSink()
        else:
            return NotImplemented

    def execute(self, our: We):
        return iter(())

    def process(self, our: We, tr_source: "TransformedSource") -> Iterator[PRecord]:
        raise NotImplementedError



class ListSourceSink(Source, Sink):
    def __init__(self, dest_list: List[PRecord]):
        self.dest_list = dest_list

    def get_hash(self) -> str:
        return str(id(self.dest_list))

    def generate_precords(self, our: We) -> Iterator[PRecord]:
        for item in self.dest_list:
            yield PRecord.from_object(item)

    def process(self, our: We, tr_source: "TransformedSource") -> Iterator[PRecord]:
        dest_list = self.dest_list
        for precord in tr_source.generate_precords(our):
            dest_list.append(precord)
            yield precord


class IterSource(Source):
    def __init__(self, it):
        self.it = it

    def generate_precords(self, our: We):
        for item in self.it:
            yield PRecord.from_object(item)

    def chain_hash(self) -> str:
        return pipex_hash("IterSource" + str(id(self.it)))


class PrintSink(Sink):
    def process(self, our: We, tr_source: "TransformedSource") -> Iterator[PRecord]:
        from pprint import pprint
        for precord in tr_source.generate_precords(our):
            pprint(precord.value)
            yield precord

    def chain_hash(self) -> str:
        return pipex_hash("PrintSink")


class IdentityTransformer(Transformer):
    _chain_hash = pipex_hash("IdentityTransformer")

    def transform(self, our: We, precords: Iterator[PRecord]) -> Iterator[PRecord]:
        return precords

    def chain_hash(self) -> str:
        return self._chain_hash


class TransformedSource(Source):
    def __init__(self, source: Source, transformer: Transformer):
        self.source = source
        self.transformer = transformer

    def flatten_chains(self, results: List[Union["PipeChain", str]]):
        self.source.flatten_chains(results)
        results.append(">>")
        self.transformer.flatten_chains(results)


    def chain_hash(self) -> str:
        return pipex_hash("TransformedSource", self.source, self.transformer)

    def with_sink(self, sink: Sink) -> "Pipeline":
        return Pipeline(self, sink)

    def generate_precords(self, our: We) -> Iterator[PRecord]:
        return self.transformer.transform(our, self.source.generate_precords(our))

    def __repr__(self):
        return "{!r} >> ({!r})".format(self.source, self.transformer)


class TransformedSink(Source, Sink):
    def __init__(self, transformer: Transformer, sink: Sink):
        self.transformer = transformer
        self.sink = sink

    def flatten_chains(self, results: List[Union["PipeChain", str]]):
        self.transformer.flatten_chains(results)
        results.append(">>")
        self.sink.flatten_chains(results)

    def generate_precords(self, our: We) -> Iterator[PRecord]:
        return iter(())

    def process(self, our: We, tr_source: TransformedSource) -> Iterator[Source]:
        return iter(())

    def chain_hash(self) -> str:
        return pipex_hash('TransformedSink', self.transformer, self.sink)

    def with_source(self, source: Source) -> "Pipeline":
        return Pipeline(TransformedSource(source, self.transformer), self.sink)

    def __repr__(self):
        return "({!r}) >> {!r}".format(self.transformer, self.sink)

    def execute(self, our: We):
        return iter(())


class Pipeline(Source):
    def __init__(self, transformed_source, sink: Sink):
        self.transformed_source = transformed_source
        self.sink = sink

    def flatten_chains(self, results: List[Union["PipeChain", str]]):
        self.transformed_source.flatten_chains(results)
        results.append(">>")
        self.sink.flatten_chains(results)

    @property
    def source(self):
        return self.transformed_source.source

    @property
    def transformer(self):
        return self.transformed_source.transformer

    def chain_hash(self) -> str:
        return pipex_hash("Pipeline", self.transformed_source, self.sink)

    def generate_precords(self, our: We) -> Iterator[PRecord]:
        return self.transformed_source.generate_precords(our)

    def execute(self, our: We):
        return self.sink.process(our, self.transformed_source)

    @classmethod
    def direct_pipeline(cls, source: Source, sink: Sink):
        return cls(TransformedSource(source, IdentityTransformer()), sink)

    def __repr__(self):
        return "{!r} >> {!r}".format(self.transformed_source, self.sink)
