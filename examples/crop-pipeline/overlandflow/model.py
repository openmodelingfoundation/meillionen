#!/usr/bin/env python3
from landlab.components.overland_flow import OverlandFlow
from landlab.components import SoilInfiltrationGreenAmpt
from meillionen.meillionen import server_respond_from_cli
from meillionen.handlers import LandLabGridHandler, PandasHandler, NetCDFSliceHandler
from meillionen.function import FuncInterfaceServer, FuncRequest
import pandas as pd
import xarray as xr

model = FuncInterfaceServer(
    sources={
        'weather': PandasHandler.from_kwargs(
            description='Daily weather data for a year',
            columns={
                'fields': [
                    {
                        "name": "rainfall__depth",
                        "data_type": "Float64"
                    }
                ]
            }
        ),
        'elevation': LandLabGridHandler.from_kwargs(
            description='Elevation model. Each cell is a sq m',
            data_type='Float64',
            dimensions=['x', 'y']
        )
    },
    sinks={
        'soil_water_infiltration__depth': NetCDFSliceHandler.from_kwargs(
            description='Surface water depth at each point in grid for each day in year',
            data_type='Float64',
            dimensions=['x', 'y', 'time']
        )
    })


# external metadata file with more detailed information


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
        swid.set({'time': t}, infiltration_depth)


def run_year_cli():
    """
    Calculates the mm of water that has infiltrated each grid cell for day of the year after a rainfall event
    in response to a request on the command line
    """
    rb = server_respond_from_cli('overlandflow', model.to_recordbatch('overlandflow'))
    print(rb.to_pandas()[['resource', 'payload']])
    args = FuncRequest.from_recordbatch(rb)
    weather = args.source('weather')
    elevation = args.source('elevation')
    surface_water_infiltation__depth = args.sink('soil_water_infiltration__depth')

    model_grid = model.source('elevation').load(elevation)
    weather = model.source('weather').load(weather)
    swid_size = {'x': model_grid.shape[0], 'y': model_grid.shape[1], 'time': weather.shape[0]}
    with model.sink('soil_water_infiltration__depth').save(surface_water_infiltation__depth, swid_size) as swid:
        run_year(
            mg=model_grid,
            weather=weather,
            swid=swid,
        )


if __name__ == '__main__':
    run_year_cli()
