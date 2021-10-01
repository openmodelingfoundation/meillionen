import os
import pathlib
from typing import Tuple

import flatbuffers
import numpy as np
import pandas as pd
import sh
from meillionen.interface.resource import Feather, OtherFile
from meillionen.interface.schema import Schemaless, PandasHandler


class DirHandler:
    RESOURCE_TYPES = [Schemaless]

    def __init__(self, name: str):
        self.schema = Schemaless()
        self.name = name

    def serialize(self, builder: flatbuffers.Builder):
        return self.schema.serialize(builder)

    def save(self, resource):
        path = resource.to_dict()['path']
        p = pathlib.Path(path)
        p.mkdir(parents=True, exist_ok=True)
        return path


def _write_plant_fwf(path, df: pd.DataFrame):
    colnames = [
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
        'plant_leaf_specific_area'
    ]
    df = df.loc[:, colnames]
    fmt = " %7.4f %7.4f %7.4f %7.4f %7.4f %7.4f %7.4f %7.4f %7.4f %7.4f %7.4f %7.4f %7.4f %7.4f %7.4f %7.4f %7.4f"
    with open(path, 'w') as f:
        np.savetxt(f, df.values, fmt=fmt)
        f.write('   Lfmax    EMP2    EMP1      PD      nb      rm      fc      tb   intot       n     lai       w      wr      wc      p1      f1    sla')


def _write_simctrl_fwf(path, df: pd.DataFrame):
    colnames = [
        'day_of_planting',
        'printout_freq'
    ]
    df = df.loc[:, colnames]
    fmt = '%6d %5d'
    with open(path, 'w') as f:
        np.savetxt(f, df.values, fmt=fmt)
        f.write('  DOYP  FROP')


def _write_soil_fwf(path, df: pd.DataFrame):
    colnames = [
        'soil_water_content_wilting_point',
        'soil_water_content_field_capacity',
        'soil_water_content_saturation',
        'soil_profile_depth',
        'soil_drainage_daily_percent',
        'soil_runoff_curve_number',
        'soil_water_storage'
    ]
    df = df.loc[:, colnames]
    fmt = '     %5.2f     %5.2f     %5.2f     %7.2f     %5.2f     %5.2f     %5.2f'
    with open(path, 'w') as f:
        np.savetxt(f, df.values, fmt=fmt)
        f.writelines([
            '       WPp       FCp       STp          DP      DRNp        CN        SWC',
            '  (cm3/cm3) (cm3/cm3) (cm3/cm3)        (cm)  (frac/d)        -       (mm)'
        ])


def _write_irrig_fwf(path, df: pd.DataFrame):
    colnames = [
        'day_of_year',
        'irrigation_depth'
    ]
    df = df.loc[:, colnames]
    fmt = '%5d %2.1f'
    with open(path, 'w') as f:
        np.savetxt(f, df.values, fmt=fmt)


def _write_weather_fwf(path, df: pd.DataFrame):
    colnames = [
        'day_of_year',
        'energy_flux',
        'temp_max',
        'temp_min',
        'rainfall_depth',
        'photosynthetic_energy_flux'
    ]
    df = df.loc[:, colnames]
    fmt = '%5d  %4.1f  %4.1f  %4.1f%6.1f              %4.1f'
    with open(path, 'w') as f:
        np.savetxt(f, df.values, fmt=fmt)


def _read_plant_fwf(path):
    colnames = [
        'day_of_year',
        'plant_leaf_count',
        'air_accumulated_temp',
        'plant_matter',
        'plant_matter_canopy',
        'plant_matter_fruit',
        'plant_matter_root',
        'plant_leaf_area_index',
    ]
    return pd.read_fwf(path, skiprows=9, names=colnames)


def _read_soil_fwf(path):
    colnames = [
        'day_of_year',
        '_radiation_solar',
        '_temperative_max',
        '_temperative_min',
        '_rain__depth',
        '_irrigation',
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
    df = pd.read_fwf(path, skiprows=6, names=colnames)
    return df.loc[:, [c for c in colnames if not c.startswith('_')]]


def run_one_year(daily: pd.DataFrame, yearly: pd.DataFrame, plant: Tuple[PandasHandler, Feather], soil: Tuple[PandasHandler, Feather], raw: Tuple[DirHandler, OtherFile]):
    plant_handler, plant_resource = plant
    soil_handler, soil_resource = soil
    raw_handler, raw_resource = raw
    inputdir = os.path.join(raw_resource.path, 'input')
    os.makedirs(inputdir, exist_ok=True)
    outputdir = os.path.join(raw_resource.path, 'output')
    os.makedirs(outputdir, exist_ok=True)

    _write_plant_fwf(os.path.join(inputdir, 'plant.inp'), yearly)
    _write_soil_fwf(os.path.join(inputdir, 'soil.inp'), yearly)
    _write_simctrl_fwf(os.path.join(inputdir, 'simctrl.inp'), yearly)
    _write_irrig_fwf(os.path.join(inputdir, 'irrig.inp'), daily)
    _write_weather_fwf(os.path.join(inputdir, 'weather.inp'), daily)

    simplecrop = sh.Command('simplecrop')
    simplecrop(_cwd=raw_resource.path)

    plant = _read_plant_fwf(os.path.join(outputdir, 'plant.out'))
    soil = _read_soil_fwf(os.path.join(outputdir, 'soil.out'))

    plant_handler.save(plant_resource, plant)
    soil_handler.save(soil_resource, soil)