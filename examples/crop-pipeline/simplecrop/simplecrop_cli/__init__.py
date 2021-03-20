from meillionen import FuncInterface
from .simplecrop_cli import run
from io import BytesIO
import pyarrow as pa
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
                            "name": "day",
                            "data_type": "Float64",
                            "nullable": false,
                            "dict_id": 0,
                            "dict_is_ordered": false
                        },
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
                            "name": "day",
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


def to_table(ipc_message: bytes) -> pd.DataFrame:
    stream = BytesIO(ipc_message)
    return pa.ipc.open_stream(stream).read_pandas()


def run_df(cli_path, dir, daily: pd.DataFrame):
    sink = pa.BufferOutputStream()
    batch = pa.RecordBatch.from_pandas(daily)
    writer = pa.ipc.new_stream(sink, batch.schema)
    writer.write_batch(batch)
    daily_ipc = sink.getvalue().to_pybytes()
    plant_ref, soil_ref = run(cli_path, dir, daily_ipc)
    return to_table(plant_ref), to_table(soil_ref)


def run_cli(cli_path):
    args = interface.to_cli()
    daily_path = args.get_source('daily')
    daily = pd.read_feather(daily_path)
    plant, soil = run_df(cli_path, 'ex', daily)

    plant_path = args.get_sink('plant')
    plant.to_feather(plant_path)

    soil_path = args.get_sink('soil')
    soil.to_feather(soil_path)