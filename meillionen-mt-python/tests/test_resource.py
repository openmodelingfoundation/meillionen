import pyarrow as pa

from meillionen.interface.resource import OtherFile
from meillionen.settings import Partitioning, Partition, Settings
from meillionen.interface.base import MethodRequestArg

settings = Settings(base_path='output')
mra = MethodRequestArg(class_name='simplecrop', method_name='run', arg_name='soil')
partitioning = Partitioning(pa.schema([pa.field('x', pa.int32()), pa.field('y', pa.int32())]))


def test_partial_csv_file():
    partial_csv = OtherFile.partial(ext='csv')
    csv = partial_csv.complete(settings=settings, mra=mra)
    assert csv.path == 'output/simplecrop/run/soil.csv'

    csv_p = partial_csv.complete(settings=settings, mra=mra, partition=partitioning.complete(x=3, y=5))
    assert csv_p.path == 'output/simplecrop/run/soil/3/5/data.csv'