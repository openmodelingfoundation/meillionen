from meillionen import PyFuncInterface
from .simplecrop_cli import run, get_func_interface
from io import BytesIO
import pyarrow as pa
import pandas as pd

interface = get_func_interface()

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