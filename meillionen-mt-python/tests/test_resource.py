from meillionen.interface.resource import OtherFile
from meillionen.settings import Settings
from meillionen.interface.base import MethodRequestArg

settings = Settings(base_path='output')
mra = MethodRequestArg(class_name='simplecrop', method_name='run', arg_name='soil')


def test_partial_csv_file():
    partial_csv = OtherFile.partial(ext='csv')
    csv = partial_csv.complete(settings=settings, mra=mra)
    assert csv.path == 'output/simplecrop/run/soil.csv'