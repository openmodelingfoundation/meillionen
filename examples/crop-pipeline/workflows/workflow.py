import itertools
from pathlib import Path

import netCDF4
from meillionen import FunctionModelCLI, file_source, feather_source, feather_sink, netcdf_sink, file_sink, parquet_sink, parquet_source
from meillionen.io import PandasLoader, PandasSaver
from meillionen.meillionen import FuncRequest
from prefect import task, Flow, Task
from prefect.engine.results import LocalResult

OUTPUT_DIR = './outputs'

overlandflow = FunctionModelCLI.from_path('../overlandflow/model.py')
simplecrop = FunctionModelCLI.from_path('simplecrop')

@task()
def run_overland_flow():
    elevation = file_source("inputs/hugo_site.asc")
    weather = feather_source("../overlandflow/rainfall.feather")
    swid = netcdf_sink({
        "type": "NetCDFResource",
        "path": f"{OUTPUT_DIR}/swid.nc",
        "variable": "soil_water_infiltration__depth",
        "data_type": "Float32",
        "slices": {}
    })

    overlandflow_req = FuncRequest()
    overlandflow_req.set_source('elevation', elevation)
    overlandflow_req.set_source('weather', weather)
    overlandflow_req.set_sink('soil_water_infiltration__depth', swid)

    overlandflow(overlandflow_req)

    return swid


@task()
def chunkify_soil_water_infiltration_depth(swid):
    swid = swid.to_dict()
    ds = netCDF4.Dataset(swid['path'], 'r')
    variable = ds[swid['variable']]
    x_d, y_d, time_d = variable.get_dims()
    return [{'soil_water_infiltration__depth': variable[x, y, :], 'x': x, 'y': y} for (x,y) in itertools.product(range(3), range(3))]


@task()
def convert_swid_chunk_to_simplecrop(c):
    return {'daily': daily, 'yearly': yearly, **c}


@task()
def simplecrop_process_chunk(data):
    yearly = data['yearly']
    daily = data['daily']
    soil_water_infiltration__depth = data['soil_water_infiltration__depth']
    x = data['x']
    y = data['y']
    d = PandasLoader.load(daily)
    d['rainfall'] = soil_water_infiltration__depth
    d_resource = parquet_source(str(Path(f'outputs/daily/{x}/{y}.feather').resolve()))
    PandasSaver.save(d_resource, d)

    plant = parquet_sink(f'outputs/plant/{x}/{y}/data.parquet')
    soil = parquet_sink(f'outputs/soil/{x}/{y}/data.parquet')
    tempdir = file_sink(f'outputs/temp/{x}/{y}')
    sc_req = FuncRequest()
    sc_req.set_source('daily', d_resource)
    sc_req.set_source('yearly', yearly)
    sc_req.set_sink('plant', plant)
    sc_req.set_sink('soil', soil)
    sc_req.set_sink('tempdir', tempdir)

    simplecrop(sc_req)


daily = feather_source(str(Path('../simplecrop/data/daily.feather').resolve()))
yearly = feather_source(str(Path('../simplecrop/data/yearly.feather').resolve()))


with Flow('crop_pipeline') as flow:
    overland_flow = run_overland_flow()
    surface_water_depth_chunks = chunkify_soil_water_infiltration_depth(overland_flow)
    simplecrop_chunks = convert_swid_chunk_to_simplecrop.map(surface_water_depth_chunks)
    yield_chunks = simplecrop_process_chunk.map(simplecrop_chunks)

flow.run()