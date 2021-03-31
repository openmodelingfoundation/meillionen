#!/usr/bin/env python3
from typing import Any, Dict

from landlab import RasterModelGrid
from landlab.components.overland_flow import OverlandFlow
from landlab.components import SoilInfiltrationGreenAmpt
from meillionen.meillionen import FuncInterface
import numpy as np
import pandas as pd
import xarray as xr

from meillionen.io import LandLabLoader, PandasLoader, NetCDF4Saver

interface = FuncInterface.from_dict({
    "name": "overlandflow",
    "sources": {
        "weather": {
            "description": "Daily weather data for a year",
            "data_type": {
                "type": "DataFrame",
                "fields": [
                    {
                        "name": "rainfall__depth",
                        "data_type": "Float64",
                        "nullable": False,
                        "dict_id": 0,
                        "dict_is_ordered": False
                    }
                ]
            }
        },
        "elevation": {
            "description": "Elevation model. Each cell is a sq m",
            "data_type": {
                "type": "Tensor",
                "label": "Elevation",
                "dimensions": ["x", "y"],
                "data_type": "Float64"
            }
        }
    },
    "sinks": {
        "soil_water_infiltration__depth": {
            "description": "Surface water depth at each point in grid for each day in year",
            "data_type": {
                "type": "Tensor",
                "label": "Surface Water Infiltration Depth",
                "dimensions": ["x", "y", "time"],
                "data_type": "Float64"
            }
        }
    }
})


def run_day(mg, precipitation: float, duration=1800):
    """
    :param dem: model grid
    :param precipitation: rainfall in m/hr
    :param duration: seconds of rainfall
    :return:
    """
    elapsed_time = 0.0
    swd = mg.add_zeros('surface_water__depth', at='node', clobber=True)
    swd += precipitation
    swid = mg.add_zeros('soil_water_infiltration__depth', at='node', clobber=True)
    swid += 1e-6
    siga = SoilInfiltrationGreenAmpt(mg)
    of = OverlandFlow(mg, steep_slopes=True)

    while elapsed_time < duration:
        dt = of.calc_time_step()
        dt = dt if dt + elapsed_time < duration else duration - elapsed_time
        of.run_one_step(dt=dt)
        siga.run_one_step(dt=dt)
        elapsed_time += dt
    return xr.DataArray(mg.at_node['soil_water_infiltration__depth'], dims=('x', 'y'))


def run_year(mg, weather, swid):
    for t, row in enumerate(weather.iterrows()):
        infiltration_depth = run_day(mg, row.precipitation)
        swid.set_slice(infiltration_depth, time=t)


if __name__ == '__main__':
    args = interface.to_cli()
    weather = args.get_source('weather')
    elevation = args.get_source('elevation')
    surface_water_infiltation__depth = args.get_sink('soil_water_infiltation__depth')

    mg = LandLabLoader.load(elevation)
    weather = PandasLoader.load(weather)
    swid_schema = interface.to_dict()['sinks']['soil_water_infiltration__depth']['dimensions']
    swid_schema = list(zip(swid_schema, [mg.shape[0], mg.shape[1], weather.shape[0]]))
    with NetCDF4Saver(surface_water_infiltation__depth, swid_schema) as swid:
        run_year(
            mg=mg,
            weather=weather,
            swid=swid,
        )