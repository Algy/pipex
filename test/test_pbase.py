import pytest

from pipex.pbase import (
    Source, Transformer, Sink,
    TransformedSource, TransformerSequence,
    TransformedSink, Pipeline
)

class MySource(Source):
    pass

class MyTransformer1(Transformer):
    pass

class MyTransformer2(Transformer):
    pass

class MyTransformer3(Transformer):
    pass

class MyTransformer4(Transformer):
    pass

class MySink(Sink):
    pass

class MySinkAndSource(Source, Sink):
    pass


def test_nested_transformer():
    tr1, tr2, tr3 = MyTransformer1(), MyTransformer2(), MyTransformer3()
    transformer = tr1 | tr2
    assert isinstance(transformer, TransformerSequence)
    assert transformer.transformers == [tr1, tr2]

    new_transformer = tr3 | transformer
    assert isinstance(new_transformer, TransformerSequence)
    assert new_transformer.transformers == [tr3, tr1, tr2]

    # Immutable
    assert transformer.transformers == [tr1, tr2]


    new_transformer = transformer | tr3
    assert new_transformer.transformers == [tr1, tr2, tr3]

    new_transformer = transformer | transformer
    assert new_transformer.transformers == [tr1, tr2, tr1, tr2]


def test_transformed_source():
    source = MySource()
    sink = MySink()
    tr1, tr2, tr3 = MyTransformer1(), MyTransformer2(), MyTransformer3()

    tr_source = source >> tr1
    assert isinstance(tr_source, TransformedSource)
    assert tr_source.source == source
    assert tr_source.transformer == tr1

    tr_source_2 = tr_source | tr2

    assert isinstance(tr_source_2, TransformedSource)
    assert tr_source_2.source == source
    assert tr_source_2.transformer.transformers == [tr1, tr2]

    pipeline = tr_source >> sink
    assert isinstance(pipeline, Pipeline)
    assert pipeline.source == source
    assert pipeline.sink == sink
    assert pipeline.transformer == tr1

    pipeline = tr_source

def test_transformed_sink():
    source = MySource()
    sink = MySink()

    tr1, tr2, tr3 = MyTransformer1(), MyTransformer2(), MyTransformer3()
    tr_sink = tr2 >> sink
    assert isinstance(tr_sink, TransformedSink)
    assert tr_sink.transformer == tr2
    assert tr_sink.sink == sink

    tr_sink_2 = tr1 | tr_sink
    assert isinstance(tr_sink_2, TransformedSink)
    assert tr_sink_2.transformer.transformers == [tr1, tr2]
    assert tr_sink_2.sink == sink

    pipeline = source >> tr_sink
    assert isinstance(pipeline, Pipeline)
    assert pipeline.source == source
    assert pipeline.sink == sink
    assert pipeline.transformer == tr2

def test_pipeline():
    source = MySource()
    sink = MySink()
    tr1, tr2, tr3 = MyTransformer1(), MyTransformer2(), MyTransformer3()

    # C(5 - 1) = 12 ways to combine
    # + one way to represent it without parantheses
    pipelines = [
        (source >> ((tr1 | ((tr2 | ((tr3 >> (sink)))))))),
        (source >> ((tr1 | (((tr2 | (tr3)) >> (sink)))))),
        (source >> (((tr1 | (tr2)) | ((tr3 >> (sink)))))),
        (source >> (((tr1 | ((tr2 | (tr3)))) >> (sink)))),
        (source >> ((((tr1 | (tr2)) | (tr3)) >> (sink)))),
        ((source >> (tr1)) | ((tr2 | ((tr3 >> (sink)))))),
        ((source >> (tr1)) | (((tr2 | (tr3)) >> (sink)))),
        ((source >> ((tr1 | (tr2)))) | ((tr3 >> (sink)))),
        (((source >> (tr1)) | (tr2)) | ((tr3 >> (sink)))),
        ((source >> ((tr1 | ((tr2 | (tr3)))))) >> (sink)),
        ((source >> (((tr1 | (tr2)) | (tr3)))) >> (sink)),
        (((source >> (tr1)) | ((tr2 | (tr3)))) >> (sink)),
        (((source >> ((tr1 | (tr2)))) | (tr3)) >> (sink)),
        ((((source >> (tr1)) | (tr2)) | (tr3)) >> (sink)),
        source >> tr1 | tr2 | tr3 >> sink,
    ]

    for pipeline in pipelines:
        assert isinstance(pipeline, Pipeline)
        assert pipeline.source == source
        assert pipeline.sink == sink
        assert pipeline.transformer.transformers == [tr1, tr2, tr3]



def test_nested_pipeline():
    source = MySource()
    sink = MySinkAndSource()
    sink_2 = MySink()
    tr1, tr2, tr3, tr4 = MyTransformer1(), MyTransformer2(), MyTransformer3(), MyTransformer4()

    p2 = tr1 | tr2 >> sink >> tr3

    assert isinstance(p2, TransformedSource)
    assert isinstance(p2.source, TransformedSink)
    assert p2.source.transformer.transformers == [tr1, tr2]
    assert p2.source.sink == sink
    assert p2.transformer == tr3

    p3 = source >> (tr1 | tr2 >> sink >> tr3)

    assert isinstance(p3, TransformedSource)
    assert isinstance(p3.source, Pipeline)
    assert p3.source.source == source
    assert p3.source.transformer.transformers == [tr1, tr2]
    assert p3.source.sink == sink

    # C(7 - 1) = 132 ways
    # + one way without parantheses
    pipelines = [
        source >> tr1 | tr2 >> sink >> tr3 | tr4 >> sink_2,
        (source >> ((tr1 | ((tr2 >> ((sink >> (((tr3 | (tr4)) >> (sink_2)))))))))),
        (source >> ((tr1 | ((tr2 >> (((sink >> (tr3)) | ((tr4 >> (sink_2)))))))))),
        (source >> ((tr1 | ((tr2 >> (((sink >> ((tr3 | (tr4)))) >> (sink_2)))))))),
        (source >> ((tr1 | ((tr2 >> ((((sink >> (tr3)) | (tr4)) >> (sink_2)))))))),
        (source >> ((tr1 | (((tr2 >> (sink)) >> ((tr3 | ((tr4 >> (sink_2)))))))))),
        (source >> ((tr1 | (((tr2 >> (sink)) >> (((tr3 | (tr4)) >> (sink_2)))))))),
        (source >> ((tr1 | (((tr2 >> ((sink >> (tr3)))) | ((tr4 >> (sink_2)))))))),
        (source >> ((tr1 | ((((tr2 >> (sink)) >> (tr3)) | ((tr4 >> (sink_2)))))))),
        (source >> ((tr1 | (((tr2 >> ((sink >> ((tr3 | (tr4)))))) >> (sink_2)))))),
        (source >> ((tr1 | (((tr2 >> (((sink >> (tr3)) | (tr4)))) >> (sink_2)))))),
        (source >> ((tr1 | ((((tr2 >> (sink)) >> ((tr3 | (tr4)))) >> (sink_2)))))),
        (source >> ((tr1 | ((((tr2 >> ((sink >> (tr3)))) | (tr4)) >> (sink_2)))))),
        (source >> ((tr1 | (((((tr2 >> (sink)) >> (tr3)) | (tr4)) >> (sink_2)))))),
        (source >> (((tr1 | (tr2)) >> ((sink >> ((tr3 | ((tr4 >> (sink_2)))))))))),
        (source >> (((tr1 | (tr2)) >> ((sink >> (((tr3 | (tr4)) >> (sink_2)))))))),
        (source >> (((tr1 | (tr2)) >> (((sink >> (tr3)) | ((tr4 >> (sink_2)))))))),
        (source >> (((tr1 | (tr2)) >> (((sink >> ((tr3 | (tr4)))) >> (sink_2)))))),
        (source >> (((tr1 | (tr2)) >> ((((sink >> (tr3)) | (tr4)) >> (sink_2)))))),
        (source >> (((tr1 | ((tr2 >> (sink)))) >> ((tr3 | ((tr4 >> (sink_2)))))))),
        (source >> (((tr1 | ((tr2 >> (sink)))) >> (((tr3 | (tr4)) >> (sink_2)))))),
        (source >> ((((tr1 | (tr2)) >> (sink)) >> ((tr3 | ((tr4 >> (sink_2)))))))),
        (source >> ((((tr1 | (tr2)) >> (sink)) >> (((tr3 | (tr4)) >> (sink_2)))))),
        (source >> (((tr1 | ((tr2 >> ((sink >> (tr3)))))) | ((tr4 >> (sink_2)))))),
        (source >> (((tr1 | (((tr2 >> (sink)) >> (tr3)))) | ((tr4 >> (sink_2)))))),
        (source >> ((((tr1 | (tr2)) >> ((sink >> (tr3)))) | ((tr4 >> (sink_2)))))),
        (source >> ((((tr1 | ((tr2 >> (sink)))) >> (tr3)) | ((tr4 >> (sink_2)))))),
        (source >> (((((tr1 | (tr2)) >> (sink)) >> (tr3)) | ((tr4 >> (sink_2)))))),
        (source >> (((tr1 | ((tr2 >> ((sink >> ((tr3 | (tr4)))))))) >> (sink_2)))),
        (source >> (((tr1 | ((tr2 >> (((sink >> (tr3)) | (tr4)))))) >> (sink_2)))),
        (source >> (((tr1 | (((tr2 >> (sink)) >> ((tr3 | (tr4)))))) >> (sink_2)))),
        (source >> (((tr1 | (((tr2 >> ((sink >> (tr3)))) | (tr4)))) >> (sink_2)))),
        (source >> (((tr1 | ((((tr2 >> (sink)) >> (tr3)) | (tr4)))) >> (sink_2)))),
        (source >> ((((tr1 | (tr2)) >> ((sink >> ((tr3 | (tr4)))))) >> (sink_2)))),
        (source >> ((((tr1 | (tr2)) >> (((sink >> (tr3)) | (tr4)))) >> (sink_2)))),
        (source >> ((((tr1 | ((tr2 >> (sink)))) >> ((tr3 | (tr4)))) >> (sink_2)))),
        (source >> (((((tr1 | (tr2)) >> (sink)) >> ((tr3 | (tr4)))) >> (sink_2)))),
        (source >> ((((tr1 | ((tr2 >> ((sink >> (tr3)))))) | (tr4)) >> (sink_2)))),
        (source >> ((((tr1 | (((tr2 >> (sink)) >> (tr3)))) | (tr4)) >> (sink_2)))),
        (source >> (((((tr1 | (tr2)) >> ((sink >> (tr3)))) | (tr4)) >> (sink_2)))),
        (source >> (((((tr1 | ((tr2 >> (sink)))) >> (tr3)) | (tr4)) >> (sink_2)))),
        (source >> ((((((tr1 | (tr2)) >> (sink)) >> (tr3)) | (tr4)) >> (sink_2)))),
        ((source >> (tr1)) | ((tr2 >> ((sink >> ((tr3 | ((tr4 >> (sink_2)))))))))),
        ((source >> (tr1)) | ((tr2 >> ((sink >> (((tr3 | (tr4)) >> (sink_2)))))))),
        ((source >> (tr1)) | ((tr2 >> (((sink >> (tr3)) | ((tr4 >> (sink_2)))))))),
        ((source >> (tr1)) | ((tr2 >> (((sink >> ((tr3 | (tr4)))) >> (sink_2)))))),
        ((source >> (tr1)) | ((tr2 >> ((((sink >> (tr3)) | (tr4)) >> (sink_2)))))),
        ((source >> (tr1)) | (((tr2 >> (sink)) >> ((tr3 | ((tr4 >> (sink_2)))))))),
        ((source >> (tr1)) | (((tr2 >> (sink)) >> (((tr3 | (tr4)) >> (sink_2)))))),
        ((source >> (tr1)) | (((tr2 >> ((sink >> (tr3)))) | ((tr4 >> (sink_2)))))),
        ((source >> (tr1)) | ((((tr2 >> (sink)) >> (tr3)) | ((tr4 >> (sink_2)))))),
        ((source >> (tr1)) | (((tr2 >> ((sink >> ((tr3 | (tr4)))))) >> (sink_2)))),
        ((source >> (tr1)) | (((tr2 >> (((sink >> (tr3)) | (tr4)))) >> (sink_2)))),
        ((source >> (tr1)) | ((((tr2 >> (sink)) >> ((tr3 | (tr4)))) >> (sink_2)))),
        ((source >> (tr1)) | ((((tr2 >> ((sink >> (tr3)))) | (tr4)) >> (sink_2)))),
        ((source >> (tr1)) | (((((tr2 >> (sink)) >> (tr3)) | (tr4)) >> (sink_2)))),
        ((source >> ((tr1 | (tr2)))) >> ((sink >> ((tr3 | ((tr4 >> (sink_2)))))))),
        ((source >> ((tr1 | (tr2)))) >> ((sink >> (((tr3 | (tr4)) >> (sink_2)))))),
        ((source >> ((tr1 | (tr2)))) >> (((sink >> (tr3)) | ((tr4 >> (sink_2)))))),
        ((source >> ((tr1 | (tr2)))) >> (((sink >> ((tr3 | (tr4)))) >> (sink_2)))),
        ((source >> ((tr1 | (tr2)))) >> ((((sink >> (tr3)) | (tr4)) >> (sink_2)))),
        (((source >> (tr1)) | (tr2)) >> ((sink >> ((tr3 | ((tr4 >> (sink_2)))))))),
        (((source >> (tr1)) | (tr2)) >> ((sink >> (((tr3 | (tr4)) >> (sink_2)))))),
        (((source >> (tr1)) | (tr2)) >> (((sink >> (tr3)) | ((tr4 >> (sink_2)))))),
        (((source >> (tr1)) | (tr2)) >> (((sink >> ((tr3 | (tr4)))) >> (sink_2)))),
        (((source >> (tr1)) | (tr2)) >> ((((sink >> (tr3)) | (tr4)) >> (sink_2)))),
        ((source >> ((tr1 | ((tr2 >> (sink)))))) >> ((tr3 | ((tr4 >> (sink_2)))))),
        ((source >> ((tr1 | ((tr2 >> (sink)))))) >> (((tr3 | (tr4)) >> (sink_2)))),
        ((source >> (((tr1 | (tr2)) >> (sink)))) >> ((tr3 | ((tr4 >> (sink_2)))))),
        ((source >> (((tr1 | (tr2)) >> (sink)))) >> (((tr3 | (tr4)) >> (sink_2)))),
        (((source >> (tr1)) | ((tr2 >> (sink)))) >> ((tr3 | ((tr4 >> (sink_2)))))),
        (((source >> (tr1)) | ((tr2 >> (sink)))) >> (((tr3 | (tr4)) >> (sink_2)))),
        (((source >> ((tr1 | (tr2)))) >> (sink)) >> ((tr3 | ((tr4 >> (sink_2)))))),
        (((source >> ((tr1 | (tr2)))) >> (sink)) >> (((tr3 | (tr4)) >> (sink_2)))),
        ((((source >> (tr1)) | (tr2)) >> (sink)) >> ((tr3 | ((tr4 >> (sink_2)))))),
        ((((source >> (tr1)) | (tr2)) >> (sink)) >> (((tr3 | (tr4)) >> (sink_2)))),
        ((source >> ((tr1 | ((tr2 >> ((sink >> (tr3)))))))) | ((tr4 >> (sink_2)))),
        ((source >> ((tr1 | (((tr2 >> (sink)) >> (tr3)))))) | ((tr4 >> (sink_2)))),
        ((source >> (((tr1 | (tr2)) >> ((sink >> (tr3)))))) | ((tr4 >> (sink_2)))),
        ((source >> (((tr1 | ((tr2 >> (sink)))) >> (tr3)))) | ((tr4 >> (sink_2)))),
        ((source >> ((((tr1 | (tr2)) >> (sink)) >> (tr3)))) | ((tr4 >> (sink_2)))),
        (((source >> (tr1)) | ((tr2 >> ((sink >> (tr3)))))) | ((tr4 >> (sink_2)))),
        (((source >> (tr1)) | (((tr2 >> (sink)) >> (tr3)))) | ((tr4 >> (sink_2)))),
        (((source >> ((tr1 | (tr2)))) >> ((sink >> (tr3)))) | ((tr4 >> (sink_2)))),
        ((((source >> (tr1)) | (tr2)) >> ((sink >> (tr3)))) | ((tr4 >> (sink_2)))),
        (((source >> ((tr1 | ((tr2 >> (sink)))))) >> (tr3)) | ((tr4 >> (sink_2)))),
        (((source >> (((tr1 | (tr2)) >> (sink)))) >> (tr3)) | ((tr4 >> (sink_2)))),
        ((((source >> (tr1)) | ((tr2 >> (sink)))) >> (tr3)) | ((tr4 >> (sink_2)))),
        ((((source >> ((tr1 | (tr2)))) >> (sink)) >> (tr3)) | ((tr4 >> (sink_2)))),
        (((((source >> (tr1)) | (tr2)) >> (sink)) >> (tr3)) | ((tr4 >> (sink_2)))),
        ((source >> ((tr1 | ((tr2 >> ((sink >> ((tr3 | (tr4)))))))))) >> (sink_2)),
        ((source >> ((tr1 | ((tr2 >> (((sink >> (tr3)) | (tr4)))))))) >> (sink_2)),
        ((source >> ((tr1 | (((tr2 >> (sink)) >> ((tr3 | (tr4)))))))) >> (sink_2)),
        ((source >> ((tr1 | (((tr2 >> ((sink >> (tr3)))) | (tr4)))))) >> (sink_2)),
        ((source >> ((tr1 | ((((tr2 >> (sink)) >> (tr3)) | (tr4)))))) >> (sink_2)),
        ((source >> (((tr1 | (tr2)) >> ((sink >> ((tr3 | (tr4)))))))) >> (sink_2)),
        ((source >> (((tr1 | (tr2)) >> (((sink >> (tr3)) | (tr4)))))) >> (sink_2)),
        ((source >> (((tr1 | ((tr2 >> (sink)))) >> ((tr3 | (tr4)))))) >> (sink_2)),
        ((source >> ((((tr1 | (tr2)) >> (sink)) >> ((tr3 | (tr4)))))) >> (sink_2)),
        ((source >> (((tr1 | ((tr2 >> ((sink >> (tr3)))))) | (tr4)))) >> (sink_2)),
        ((source >> (((tr1 | (((tr2 >> (sink)) >> (tr3)))) | (tr4)))) >> (sink_2)),
        ((source >> ((((tr1 | (tr2)) >> ((sink >> (tr3)))) | (tr4)))) >> (sink_2)),
        ((source >> ((((tr1 | ((tr2 >> (sink)))) >> (tr3)) | (tr4)))) >> (sink_2)),
        ((source >> (((((tr1 | (tr2)) >> (sink)) >> (tr3)) | (tr4)))) >> (sink_2)),
        (((source >> (tr1)) | ((tr2 >> ((sink >> ((tr3 | (tr4)))))))) >> (sink_2)),
        (((source >> (tr1)) | ((tr2 >> (((sink >> (tr3)) | (tr4)))))) >> (sink_2)),
        (((source >> (tr1)) | (((tr2 >> (sink)) >> ((tr3 | (tr4)))))) >> (sink_2)),
        (((source >> (tr1)) | (((tr2 >> ((sink >> (tr3)))) | (tr4)))) >> (sink_2)),
        (((source >> (tr1)) | ((((tr2 >> (sink)) >> (tr3)) | (tr4)))) >> (sink_2)),
        (((source >> ((tr1 | (tr2)))) >> ((sink >> ((tr3 | (tr4)))))) >> (sink_2)),
        (((source >> ((tr1 | (tr2)))) >> (((sink >> (tr3)) | (tr4)))) >> (sink_2)),
        ((((source >> (tr1)) | (tr2)) >> ((sink >> ((tr3 | (tr4)))))) >> (sink_2)),
        ((((source >> (tr1)) | (tr2)) >> (((sink >> (tr3)) | (tr4)))) >> (sink_2)),
        (((source >> ((tr1 | ((tr2 >> (sink)))))) >> ((tr3 | (tr4)))) >> (sink_2)),
        (((source >> (((tr1 | (tr2)) >> (sink)))) >> ((tr3 | (tr4)))) >> (sink_2)),
        ((((source >> (tr1)) | ((tr2 >> (sink)))) >> ((tr3 | (tr4)))) >> (sink_2)),
        ((((source >> ((tr1 | (tr2)))) >> (sink)) >> ((tr3 | (tr4)))) >> (sink_2)),
        (((((source >> (tr1)) | (tr2)) >> (sink)) >> ((tr3 | (tr4)))) >> (sink_2)),
        (((source >> ((tr1 | ((tr2 >> ((sink >> (tr3)))))))) | (tr4)) >> (sink_2)),
        (((source >> ((tr1 | (((tr2 >> (sink)) >> (tr3)))))) | (tr4)) >> (sink_2)),
        (((source >> (((tr1 | (tr2)) >> ((sink >> (tr3)))))) | (tr4)) >> (sink_2)),
        (((source >> (((tr1 | ((tr2 >> (sink)))) >> (tr3)))) | (tr4)) >> (sink_2)),
        (((source >> ((((tr1 | (tr2)) >> (sink)) >> (tr3)))) | (tr4)) >> (sink_2)),
        ((((source >> (tr1)) | ((tr2 >> ((sink >> (tr3)))))) | (tr4)) >> (sink_2)),
        ((((source >> (tr1)) | (((tr2 >> (sink)) >> (tr3)))) | (tr4)) >> (sink_2)),
        ((((source >> ((tr1 | (tr2)))) >> ((sink >> (tr3)))) | (tr4)) >> (sink_2)),
        (((((source >> (tr1)) | (tr2)) >> ((sink >> (tr3)))) | (tr4)) >> (sink_2)),
        ((((source >> ((tr1 | ((tr2 >> (sink)))))) >> (tr3)) | (tr4)) >> (sink_2)),
        ((((source >> (((tr1 | (tr2)) >> (sink)))) >> (tr3)) | (tr4)) >> (sink_2)),
        (((((source >> (tr1)) | ((tr2 >> (sink)))) >> (tr3)) | (tr4)) >> (sink_2)),
        (((((source >> ((tr1 | (tr2)))) >> (sink)) >> (tr3)) | (tr4)) >> (sink_2)),
        ((((((source >> (tr1)) | (tr2)) >> (sink)) >> (tr3)) | (tr4)) >> (sink_2)),
    ]

    for pipeline in pipelines:
        assert isinstance(pipeline, Pipeline)
        assert pipeline.sink == sink_2
        assert pipeline.transformer.transformers == [tr3, tr4]

        assert isinstance(pipeline.source, Pipeline)
        assert pipeline.source.transformer.transformers == [tr1, tr2]
        assert pipeline.source.sink == sink
        assert pipeline.source.source == source
