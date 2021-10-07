import pytest

from meillionen.interface.method_request import MethodRequest
from meillionen.interface.resource import Feather, NetCDF, OtherFile
from meillionen.interface.schema import NetCDFHandler
from meillionen.settings import Settings
from meillionen.client import Client, CLIRef


def test_cli():
    ref = CLIRef('overlandflow-omf')
    settings = Settings(base_path='output')
    client = Client(module_ref=ref, settings=settings)
    kwargs = client.run(
        class_name='overlandflow',
        method_name='run',
        resource_payloads={
            'weather': Feather('../workflows/inputs/weather.feather'),
            'elevation': OtherFile('../workflows/inputs/elevation.asc'),
            'soil_water_infiltration__depth': NetCDF.partial(variable='swid')
        }
    )
