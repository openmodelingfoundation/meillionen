from meillionen import FuncInterface
from .simplecrop_cli import run

import pandas as pd


interface = FuncInterface.from_json('''{
    "name": "simplecrop",
    "sources": {
        "daily": {
            "description": "daily data",
            "datatype": {
                "Table": {
                    "fields": [
                        {
                            "name": "irrigation",
                            "data_type": "Float64",
                            "nullable": false,
                            "dict_id": 0,
                            "dict_is_ordered": false
                        },
                        {
                            "name": "temp_max",
                            "data_type": "Float64",
                            "nullable": false,
                            "dict_id": 0,
                            "dict_is_ordered": false
                        },
                        {
                            "name": "temp_min",
                            "data_type": "Float64",
                            "nullable": false,
                            "dict_id": 0,
                            "dict_is_ordered": false
                        },
                        {
                            "name": "rainfall",
                            "data_type": "Float64",
                            "nullable": false,
                            "dict_id": 0,
                            "dict_is_ordered": false
                        },
                        {
                            "name": "photosynthetic_energy_flux",
                            "data_type": "Float64",
                            "nullable": false,
                            "dict_id": 0,
                            "dict_is_ordered": false
                        },
                        {
                            "name": "energy_flux",
                            "data_type": "Float64",
                            "nullable": false,
                            "dict_id": 0,
                            "dict_is_ordered": false
                        }
                    ],
                    "metadata": {}
                }
            }
        }
    },
    "sinks": {
        "crop_yields": {
            "description": "crop yields",
            "datatype": {
                "Table": {
                    "fields": [
                        {
                            "name": "day_of_year",
                            "data_type": "Int64",
                            "nullable": false,
                            "dict_id": 0,
                            "dict_is_ordered": false
                        },
                        {
                            "name": "yield",
                            "data_type": "Float64",
                            "nullable": false,
                            "dict_id": 0,
                            "dict_is_ordered": false
                        }
                    ],
                    "metadata": {}
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