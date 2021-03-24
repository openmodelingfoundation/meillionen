from meillionen import feather_sink, feather_source
from meillionen.io import PandasLoader
import pytest


def test_load_feather():
    source = feather_sink("../examples/crop-pipeline/simplecrop/data/yearly.feather")
    df = PandasLoader.load(source)
    assert df.shape == (1, 26)