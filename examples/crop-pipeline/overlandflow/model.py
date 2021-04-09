#!/usr/bin/env python3
from landlab.components.overland_flow import OverlandFlow
from landlab.components import SoilInfiltrationGreenAmpt
from meillionen.meillionen import FuncInterface
from meillionen.io import LandLabLoader, PandasLoader, NetCDF4Saver
import pandas as pd
import xarray as xr


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
    Calculates the mm of water that has infiltrated into soil after a rainfall event.
    Rainfall events are assumed to be instantaneous and constant over the model grid.

    :param dem: model grid
    :param precipitation: rainfall in mm/hr
    :param duration: seconds of rainfall
    :return: mm of water infiltrated into the soil at every coordinate in the grid
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
    return xr.DataArray(mg.at_node['soil_water_infiltration__depth'].reshape(mg.shape), dims=('x', 'y'))


def run_year(mg, weather: pd.DataFrame, swid):
    """
    Calculates the mm of water that has infiltrated each grid cell for day of the year after a rainfall event

    :param mg: model grid
    :param weather: total rainfall in mm each day of a year
    :param swid: reference to data cube with x, y, time dimensions used to incrementally save model results
    """
    for t, row in weather.iterrows():
        infiltration_depth = run_day(mg, row.rainfall__depth)
        swid.set_slice(infiltration_depth, time=t)


def run_year_cli():
    """
    Calculates the mm of water that has infiltrated each grid cell for day of the year after a rainfall event
    in response to a request on the command line
    """
    args = interface.to_cli()
    weather = args.get_source('weather')
    elevation = args.get_source('elevation')
    surface_water_infiltation__depth = args.get_sink('soil_water_infiltration__depth')

    mg = LandLabLoader.load(elevation)
    weather = PandasLoader.load(weather)
    swid_schema = interface.to_dict()['sinks']['soil_water_infiltration__depth']['data_type']['dimensions']
    # create dimension label / size parts [('x', 10), ('y', 10), ('time', 365')]
    swid_schema = list(zip(swid_schema, [mg.shape[0], mg.shape[1], weather.shape[0]]))
    with NetCDF4Saver(surface_water_infiltation__depth, swid_schema) as swid:
        run_year(
            mg=mg,
            weather=weather,
            swid=swid,
        )


if __name__ == '__main__':
    run_year_cli()