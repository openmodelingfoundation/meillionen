from meillionen.interface.method_request import MethodRequest
from meillionen.interface.resource import Feather, OtherFile
from simplecrop_omf.model import server
from simplecrop_omf.io import \
    _write_irrig_fwf,\
    _write_plant_fwf,\
    _write_simctrl_fwf,\
    _write_soil_fwf,\
    _write_weather_fwf, \
    run_one_year
import pandas as pd


def test_write_yearly():
    yearly = pd.read_feather('data/yearly.feather')
    _write_plant_fwf('plant.inp', yearly)
    _write_simctrl_fwf('simctrl.inp', yearly)
    _write_soil_fwf('soil.inp', yearly)


def test_write_daily():
    daily = pd.read_feather('data/daily.feather')
    _write_irrig_fwf('irrig.inp', daily)
    _write_weather_fwf('weather.inp', daily)


def test_run_one_year():
    sources = {
        'daily': Feather('data/daily.feather'),
        'yearly': Feather('data/yearly.feather')
    }
    sinks = {
        'plant': Feather('output/plant.feather'),
        'soil': Feather('output/soil.feather'),
        'raw': OtherFile('output/raw')
    }
    mr = MethodRequest(
        class_name='simplecrop',
        method_name='run',
        kwargs={
            **sources,
            **sinks
        }
    )
    server.apply(mr)