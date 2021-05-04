import os
import pathlib

from meillionen.meillionen import server_respond_from_cli
from meillionen.resource import FuncInterfaceServer, FuncRequest, PandasLoaderSaver, Unvalidated
from .simplecrop_omf import run
from io import BytesIO
import pyarrow as pa
import pandas as pd


class MkDirSaver:
    RESOURCE_TYPES = [Unvalidated]

    def __init__(self):
        self.validator = Unvalidated()

    def save(self, resource):
        path = resource.to_dict()['path']
        p = pathlib.Path(path)
        p.mkdir(parents=True, exist_ok=True)
        return path


interface = FuncInterfaceServer(
    sources={
        'daily': PandasLoaderSaver.from_kwargs(
            description="Daily inputs that influence crop yield",
            columns={
                'fields': [
                    {
                        'name': name,
                        'data_type': 'Float32'
                    } for name in
                    [
                        'irrigation',
                        'temp_max',
                        'temp_min',
                        'rainfall',
                        'photosynthetic_energy_flux',
                        'energy_flux',
                    ]
                ]
            }
        ),
        'yearly': PandasLoaderSaver.from_kwargs(
            description='Yearly parameters influencing crop growth',
            columns={
                'fields': [
                    {
                        'name': name,
                        'data_type': 'Float32'
                    } for name in
                    [
                        'plant_leaves_max_number',
                        'plant_emp2',
                        'plant_emp1',
                        'plant_density',
                        'plant_nb',
                        'plant_leaf_max_appearance_rate',
                        'plant_growth_canopy_fraction',
                        'plant_min_repro_growth_temp',
                        'plant_repro_phase_duration',
                        'plant_leaves_number_of',
                        'plant_leaf_area_index',
                        'plant_matter',
                        'plant_matter_root',
                        'plant_matter_canopy',
                        'plant_matter_leaves_removed',
                        'plant_development_phase',
                        'plant_leaf_specific_area',

                        'soil_water_content_wilting_point',
                        'soil_water_content_field_capacity',
                        'soil_water_content_saturation',
                        'soil_profile_depth',
                        'soil_drainage_daily_percent',
                        'soil_runoff_curve_number',
                        'soil_water_storage'
                    ]
                ] + [
                    {
                        'name': 'day_of_planting',
                        'data_type': 'Int32'
    ,
                    },
                    {
                        'name': 'printout_freq',
                        'data_type': 'Int32'
                    }
                ]
            }
        )
    },
    sinks={
        'plant': PandasLoaderSaver.from_kwargs(
            description='Daily plant characteristic results',
            columns={
                'fields': [
                    {
                        'name': name,
                        'data_type': 'Float32'
                    } for name in
                    [
                        'day_of_year',
                        'plant_leaf_count',
                        'air_accumulated_temp',
                        'plant_matter',
                        'plant_matter_canopy',
                        'plant_matter_fruit',
                        'plant_matter_root',
                        'plant_leaf_area_index'
                    ]
                ]
            }
        ),
        'soil': PandasLoaderSaver.from_kwargs(
            description='Daily soil characteristics',
            columns={
                'fields': [
                    {
                        'name': name,
                        'data_type': 'Float32'
                    } for name in
                    [
                        'day_of_year',
                        'soil_daily_runoff',
                        'soil_daily_infiltration',
                        'soil_daily_drainage',
                        'soil_evapotranspiration',
                        'soil_evaporation',
                        'plant_potential_transpiration',
                        'soil_water_storage_depth',
                        'soil_water_profile_ratio',
                        'soil_water_deficit_stress',
                        'soil_water_excess_stress'
                    ]
                ]
            }
        ),
        'tempdir': MkDirSaver()
    }
)


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
    rb = server_respond_from_cli(cli_path, interface.to_recordbatch(cli_path))
    args = FuncRequest.from_recordbatch(rb)
    daily: pd.DataFrame = interface.source('daily').load(args.source('daily'))
    yearly: pd.DataFrame = interface.source('yearly').load(args.source('yearly'))
    tempdir = interface.sink('tempdir').save(args.sink('tempdir'))
    plant, soil = simplecrop_mock_ipc_run(cli_path, tempdir, daily, yearly)
    interface.sink('plant').save(args.sink('plant'), plant)
    interface.sink('soil').save(args.sink('soil'), soil)