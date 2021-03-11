from meillionen import PyFuncInterface
from .simplecrop_cli import run

import pandas as pd


interface = PyFuncInterface.from_json('''{
    "name": "simplecrop",
    "sources": {
        "daily": {
            "description": "daily data",
            "datatype": {
                "Table": {
                     "irrigation": "F64",
                     "temp_max": "F64",
                     "temp_min": "F64",
                     "rainfall": "F64",
                     "photosynthetic_energy_flux": "F64",
                     "energy_flux": "F64"
                }
            }
        }
    },
    "sinks": {
        "crop_yields": {
            "description": "crop yields",
            "datatype": {
                "Table": {
                    "day_of_year": "I64",
                    "yield": "F64"
                }
            }
        }
    }
}''')


def run_cli(cli_path):
    args = interface.to_cli()
    daily_path = args.get_source('daily')
    daily = pd.read_parquet(daily_path)
    run(cli_path, daily)
    yield_path = args.get_sink('yield')
    yields = pd.read_csv('output/plant.out', names=[
        'day_of_year',
        'leaf_nodes__count',
        'temperature_accum',
        'plant__weight',
        'canopy__weight',
        'root__weight',
        'fruit__weight',
        'leaf_area_index'
    ], skiprows=9)
    yields.to_parquet(yield_path)