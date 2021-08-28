import os
import pathlib

from meillionen.interface.module_interface import ModuleInterface
from meillionen.app import App
from meillionen.interface.class_interface import ClassInterface
from meillionen.interface.method_interface import MethodInterface
from meillionen.interface.schema import PandasHandler, LandLabGridHandler, NetCDFSliceHandler
from meillionen.resource import Schemaless
from .io import run_one_year
import pyarrow as pa


class MkDirSaver:
    RESOURCE_TYPES = [Schemaless]

    def __init__(self):
        self.schema = Schemaless("")

    def save(self, resource):
        path = resource.to_dict()['path']
        p = pathlib.Path(path)
        p.mkdir(parents=True, exist_ok=True)
        return path


_run_one_year = MethodInterface(
    name='run',
    sources=[
        PandasHandler(
            name='daily',
            s=pa.schema([
                pa.field(name=name, type=pa.float32(), bool_nullable=False)
                for name in [
                    'irrigation',
                    'temp_max',
                    'temp_min',
                    'rainfall',
                    'photosynthetic_energy_flux',
                    'energy_flux'
                ]
            ])),
        PandasHandler(
            name='yearly',
            s=pa.schema([
                pa.field(name=name, type=pa.float32(), bool_nullable=False)
                for name in [
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
                ] + [
                    pa.field(name='day_of_planting', type=pa.int32(), bool_nullable=False),
                    pa.field(name='printout_freq', type=pa.int32(), bool_nullable=False)
                ]
            ])
        )
    ],
    sinks=[
        PandasHandler(
            name='plant',
            s=pa.schema([
                pa.field(name=name, type=pa.float32(), bool_nullable=False)
                for name in
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
            ])
        ),
        PandasHandler(
            name='soil',
            s=pa.schema([
                pa.field(name=name, type=pa.float32(), bool_nullable=False)
                for name in
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
            ])
        ),
        MkDirSaver(name='tempdir')
    ],
    handler=run_one_year
)
simplecrop = ClassInterface(
    name='simplecrop',
    methods=[
        _run_one_year
    ]
)
crops = ModuleInterface(
    classes=[
        simplecrop
    ]
)
app = App(crops)


if __name__ == '__main__':
    app.cli()