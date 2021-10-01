#!/usr/bin/env python3
from typing import Tuple

from landlab.components.overland_flow import OverlandFlow
from landlab.components import SoilInfiltrationGreenAmpt
from meillionen.interface.resource import NetCDF
from meillionen.server import Server
from meillionen.interface.method_interface import MethodInterface
from meillionen.interface.class_interface import ClassInterface
from meillionen.interface.module_interface import ModuleInterface
from meillionen.interface.mutability import Mutability
from meillionen.interface.schema import PandasHandler, LandLabGridHandler, NetCDFSliceHandler
import pyarrow as pa
import pandas as pd
import xarray as xr


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


def run_year_handler(elevation, weather, soil_water_infiltration__depth: Tuple[NetCDFSliceHandler, NetCDF]):
    """
    Calculates the mm of water that has infiltrated each grid cell for day of the year after a rainfall event
    in response to a request on the command line
    """

    # determine the size of the spatial daily infiltration array
    swid_size = {'x': elevation.shape[0], 'y': elevation.shape[1], 'time': weather.shape[0]}

    swid_handler, swid_resource = soil_water_infiltration__depth
    # write to the spatial daily infiltration netcdf array
    with swid_handler.save(swid_resource, swid_size) as swid:
        run_year(
            mg=elevation,
            weather=weather,
            swid=swid,
        )


settings = PandasHandler(
    name='settings',
    s=pa.schema([
        pa.field(name='soil_type', type=pa.string(), nullable=False)
    ]),
    mutability=Mutability.read
)


run = MethodInterface(
    name='run',
    args=[
        PandasHandler(
            name='weather',
            s=pa.schema([
                pa.field(name='rainfall__depth', type=pa.float64(), nullable=False)
            ]),
            mutability=Mutability.read
        ),
        LandLabGridHandler(
            name='elevation',
            data_type='f8',
            mutability=Mutability.read
        ),
        NetCDFSliceHandler(
            name='soil_water_infiltration__depth',
            data_type='f8',
            dimensions=['x', 'y', 'time'],
            mutability=Mutability.write
        )
    ],
    handler=run_year_handler
)


default_settings = MethodInterface(
    name='default_settings',
    args=[
        settings
    ]
)


overlandflow_module = ModuleInterface(
    classes=[
        ClassInterface(
            name='overlandflow',
            methods=[
                run,
                # default_settings
            ]
        )
    ]
)

# external metadata file with more detailed information


server = Server(overlandflow_module)


def main():
    server.cli()

