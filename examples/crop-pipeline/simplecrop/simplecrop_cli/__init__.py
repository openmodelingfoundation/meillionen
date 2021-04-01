import os

from meillionen.io import PandasLoader, PandasSaver
from meillionen.meillionen import FuncInterface
from .simplecrop_cli import run, get_func_interface
from io import BytesIO
import pyarrow as pa
import pandas as pd

interface = FuncInterface.from_dict(get_func_interface())


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


def run_cli():
    cli_path = os.environ.get('SIMPLECROP', 'simplecrop')
    args = interface.to_cli()
    daily: pd.DataFrame = PandasLoader.load(args.get_source('daily'))
    yearly: pd.DataFrame = PandasLoader.load(args.get_source('yearly'))
    tempdir = args.get_sink('tempdir').to_dict()['path']
    plant, soil = simplecrop_mock_ipc_run(cli_path, tempdir, daily, yearly)
    PandasSaver.save(args.get_sink('plant'), plant)
    PandasSaver.save(args.get_sink('soil'), soil)