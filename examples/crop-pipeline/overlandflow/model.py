#!/usr/bin/env python3

import numpy as np
from landlab import RasterModelGrid
from landlab.components.overland_flow import OverlandFlow
from meillionen import FuncInterface


interface = FuncInterface.from_json('''{
    "name": "overlandflow",
    "sources": {
        "weather": {
            "description": "Daily weather data for a year",
            "datatype": {
                "Table": {
                    "rainfall__depth": "F64"
                }
            } 
        },
        "topography": {
            "description": "Elevation model. Each cell is a sq m",
            "datatype": {
                "Table": {
                    "elevation": "F64"
                }
            }
        }
    },
    "sinks": {
        "surface_water": {
            "description": "Surface water depth at each point in grid for each day in year",
            "datatype": {
                "Table": {
                    "surface_water__depth": "F64"
                }
            }
        }
    }
}''')


class Overland:
    def __init__(self):
        self.shape = (20, 20)
        self.shape_used = (range(2, self.shape[0] - 2), range(2, self.shape[1] - 2))
        self.input = {}
        self.store = None

    def set_value(self, name, value):
        if name != 'rainfall__depth':
            raise KeyError('"rainfall__depth" is the only valid setter')
        self.input[name] = value

    def get_value(self, name):
        if name != 'surface_water__depth':
            raise KeyError('"surface_water__depth" is the only valid getter')
        return self.store.get_f64_variable('surface_water__depth')

    def initialize(self):
        # this example uses the python netcdf library to create a store for other components to consume
        # more complete versions of meillionen have a unified API for saving data to a store
        self.surface_water__depth = outfile.createVariable('surface_water__depth', 'f8', ('x', 'y', 'time'))
        self.output = outfile

    def finalize(self):
        self.output.close()

    def run(self, rainfall_mm):
        grid = RasterModelGrid(self.shape)
        topo = np.zeros(self.shape)
        topo[4, 5] = 0.2
        topo[13, 3] = 0.1
        grid.at_node['topographic__elevation'] = topo
        grid.at_node['surface_water__depth'] = np.zeros(self.shape)

        elapsed_time = 0.0
        model_run_time = 10.0

        storm_duration_s = 10.0
        of = OverlandFlow(grid, steep_slopes=True, rainfall_intensity=rainfall_mm * (1 / storm_duration_s * 1 / 1000))

        while elapsed_time < model_run_time:
            of.dt = of.calc_time_step()  # Adaptive time step

            if elapsed_time > storm_duration_s:
                of.rainfall_intensity = 0.0

            of.overland_flow()
            elapsed_time += of.dt

        xs, ys = self.shape_used

        return grid.at_node['surface_water__depth'].reshape(self.shape)[xs[0]:xs[1], ys[0]:ys[1]]

    def update(self):
        rainfall = self.input['rainfall__depth']
        for ind in range(365):
            if ind in rainfall.index:
                grid = self.run(rainfall[ind])
                self.surface_water__depth[:, :, ind] = grid
            else:
                xsize = len(self.shape_used[0])
                ysize = len(self.shape_used[1])
                grid = np.zeros((xsize, ysize))
                self.surface_water__depth[:, :, ind] = grid


if __name__ == '__main__':
    args = interface.to_cli()
    # import argparse
    # import pandas as pd
    #
    # parser = argparse.ArgumentParser(description='Overland Flow')
    # parser.add_argument('path', type=str, help='daily weather data for a year')
    #
    # args = parser.parse_args()
    #
    # weather = pd.read_fwf(args.path)
    #
    # # find the days that have positive precipitation
    #
    # rainfall = weather['rain'][weather['rain'] > 0]
    # rainfall = rainfall[rainfall.index.isin(range(0, 365))]
    #
    # overland = Overland()
    # overland.initialize()
    # overland.set_value('rainfall__depth', rainfall)
    # overland.update()
    # overland.finalize()