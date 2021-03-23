from meillionen import PyFuncInterface
from .simplecrop_cli import run, get_func_interface
from io import BytesIO
import pyarrow as pa
import pandas as pd

interface = get_func_interface()


def to_table(ipc_message: bytes) -> pd.DataFrame:
    stream = BytesIO(ipc_message)
    return pa.ipc.open_stream(stream).read_pandas()


def to_ipc(df: pd.DataFrame) -> pd.DataFrame:
    sink = pa.BufferOutputStream()
    batch = pa.RecordBatch.from_pandas(df)
    writer = pa.ipc.new_stream(sink, batch.schema)
    writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def simplecrop_mock_ipc_run(cli_path, dir, daily: pd.DataFrame, yearly: pd.DataFrame):
    """Run the simplecrop model as if you were sending and receiving ipc messages"""
    daily_ipc = to_ipc(daily)
    yearly_ipc = to_ipc(yearly)
    plant_ref, soil_ref = run(cli_path, dir, daily_ipc, yearly_ipc)
    return to_table(plant_ref), to_table(soil_ref)


def run_cli(cli_path):
    args = interface.to_cli()
    daily_path = args.get_source('daily')
    daily = pd.read_feather(daily_path)
    plant, soil = simplecrop_mock_ipc_run(cli_path, 'ex', daily)

    plant_path = args.get_sink('plant')
    plant.to_feather(plant_path)

    soil_path = args.get_sink('soil')
    soil.to_feather(soil_path)