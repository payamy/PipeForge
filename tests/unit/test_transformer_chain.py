"""The chain logic is exercised with plain Python objects standing in for DataFrames."""
from pipeforge.connector.base import BaseConnector
from pipeforge.transformer import Transformer


class AddA(Transformer):
    def transform(self, df):
        return df + ["a"]


class AddB(Transformer):
    def transform(self, df):
        return df + ["b"]


class AddC(Transformer):
    def transform(self, df):
        return df + ["c"]


class _Conn(BaseConnector):
    def migrate(self, full_refresh=False, **kw):
        pass

    def validate_connection(self):
        pass


def make(transformers):
    return _Conn(engine=None, sink=None, cursor_fields=["u"], transformers=transformers)


def test_chain_applies_all_transformers_in_order():
    assert make([AddA, AddB, AddC])._handle_transforms([]) == ["a", "b", "c"]


def test_repeated_runs_keep_the_first_transformer():
    conn = make([AddA, AddB])
    results = [conn._handle_transforms([]) for _ in range(3)]
    assert results == [["a", "b"]] * 3          # used to drop 'a' after the first call
    assert conn._transformers == [AddA, AddB]


def test_input_list_is_not_mutated_and_empty_chain_is_identity():
    original = [AddA, AddB]
    conn = make(original)
    conn._handle_transforms([])
    assert original == [AddA, AddB]
    sentinel = object()
    assert make(None)._handle_transforms(sentinel) is sentinel
    assert make([AddA])._handle_transforms([]) == ["a"]
