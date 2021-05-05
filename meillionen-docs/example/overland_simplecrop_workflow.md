---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.10.3
kernelspec:
  display_name: Python 3
  language: python
  name: python3
---

# Combining Overland Flow and Simple Crop

## Running Overland Flow on its Own

The overland flow model routes water over a landscape and determines how much water infiltrates particular cells in response to rain events and elevation data. Rainfall in this model is assumed to occur instantaneously and rainfall events are assumed to be independent (so a large rainfall event on a previous day has no bearing on the present day).

In order to run the model we first need to import some source and sink types as well as classes for calling off to CLI models and building requests.

```{code-cell} ipython3
import itertools
import os.path
import netCDF4
from meillionen.client import ClientFunctionModel
from meillionen.resource import PandasHandler, FuncRequest
from meillionen.resource import \
    FileResource, FeatherResource, FeatherResource, NetCDFResource, ParquetResource
from prefect import task, Flow


os.environ['SIMPLECROP'] = 'simplecrop'
os.environ['RUST_BACKTRACE'] = '1'
BASE_DIR = '../../examples/crop-pipeline'

INPUT_DIR = os.path.join(BASE_DIR, 'workflows/inputs')
OUTPUT_DIR = os.path.join(BASE_DIR, 'workflows/outputs')
```

 We also need to create sources and sinks to describe our data

```{code-cell} ipython3
elevation = FileResource(os.path.join(INPUT_DIR, "hugo_site.asc"))

weather = FeatherResource(os.path.join(INPUT_DIR, "rainfall.feather"))

swid = NetCDFResource(
    path=os.path.join(OUTPUT_DIR, "swid.nc"),
    variable="soil_water_infiltration__depth",
    dimensions=["x", "y", "time"]
)
```

and build a request to call our model with.

```{code-cell} ipython3
sources = {
    'elevation': elevation,
    'weather': weather
}

sinks = {
    'soil_water_infiltration__depth': swid
}

overlandflow = ClientFunctionModel.from_path(path=os.path.join(BASE_DIR, 'overlandflow/model.py'))
```

Then call the overland flow model with our request (which will create files on the file system)

```{code-cell} ipython3
overlandflow(sources=sources, sinks=sinks)
```

## Running Simple Crop on its Own

`simplecrop`  is a model of yearly crop growth that operates on the command line. When called in the current directory with no arguments it expects that there is an data folder to be present. It expects that the data folder contains five files

- `irrig.inp` - daily irrigation
- `plant.inp` - plant growth parameters for the simulation
- `simctrl.inp` - simulation reporting parameters
- `soil.inp` - soil characteristic parameters for the simulation
- `weather.inp` - daily weather data (with variables like maximum temperature, solar energy flux)

`simplecrop` also expects there to be an output folder. After the model has run it will populate the output folder with three files

- `plant.out`- daily plant characteristics
- `soil.out` - daily soil characteristics
- `wbal.out` - summary soil and plant statistics about simulation

In order to wrap this model in an interface that allows you to run the model without manually building those input files and manually converting the output files into a format conducive to analysis we need to have a construct the input files and parse the output files. Fortunately we have such a model wrapper already.

```{code-cell} ipython3
---
pycharm:
  name: '#%%

    '
---
from meillionen.client import ClientFunctionModel
import pandas as pd

run_simple_crop = ClientFunctionModel.from_path('simplecrop_omf')
```

```{code-cell} ipython3
daily = FeatherResource(os.path.join(BASE_DIR, 'simplecrop/data/daily.feather'))
yearly = FeatherResource(os.path.join(BASE_DIR, 'simplecrop/data/yearly.feather'))
plant = FeatherResource(os.path.join(OUTPUT_DIR, 'plant.feather'))
soil = FeatherResource(os.path.join(OUTPUT_DIR, 'soil.feather'))
tempdir = FileResource('tmp')

sources = {
    'daily': daily,
    'yearly': yearly
}

sinks = {
    'plant': plant,
    'soil': soil,
    'tempdir': tempdir
}
```

```{code-cell} ipython3
---
pycharm:
  name: '#%%

    '
---
run_simple_crop(sources=sources, sinks=sinks)
```

```{code-cell} ipython3
---
pycharm:
  name: '#%%

    '
---
soil_df = pd.read_feather(os.path.join(OUTPUT_DIR, 'soil.feather'))
soil_df
```

## Using Prefect to Feed Overland Flow Results into Simple Crop

```{code-cell} ipython3
---
pycharm:
  name: '#%%

    '
---
overlandflow = ClientFunctionModel.from_path(os.path.join(BASE_DIR, 'overlandflow/model.py'))
simplecrop = ClientFunctionModel.from_path('simplecrop_omf')


@task()
def run_overland_flow():
    elevation = FileResource(os.path.join(INPUT_DIR, "hugo_site.asc"))
    weather = FeatherResource(os.path.join(INPUT_DIR, "rainfall.feather"))
    swid = NetCDFResource(
        path=f"{OUTPUT_DIR}/swid.nc",
        variable="soil_water_infiltration__depth",
        dimensions=['x', 'y', 'time']
    )

    sources = {
        'elevation': elevation,
        'weather': weather
    }
    sinks = {
        'soil_water_infiltration__depth': swid
    }

    overlandflow(sources=sources, sinks=sinks)

    return swid


@task()
def chunkify_soil_water_infiltration_depth(swid):
    swid = swid.to_dict()
    ds = netCDF4.Dataset(swid['path'], 'r')
    variable = ds[swid['variable']]
    x_d, y_d, time_d = variable.get_dims()
    return [{'soil_water_infiltration__depth': variable[x, y, :], 'x': x, 'y': y} for (x,y) in itertools.product(range(10,12), range(21,23))]


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
    df = PandasHandler(simplecrop.source('daily')).load(daily)
    df['rainfall'] = soil_water_infiltration__depth
    daily_resource = FeatherResource(os.path.join(OUTPUT_DIR, f'outputs/daily/{x}/{y}.feather'))
    PandasHandler(simplecrop.source('daily')).save(daily_resource, data=df)

    plant = ParquetResource(os.path.join(OUTPUT_DIR, f'plant/{x}/{y}/data.parquet'))
    soil = ParquetResource(os.path.join(OUTPUT_DIR, f'soil/{x}/{y}/data.parquet'))
    tempdir = FileResource(os.path.join(OUTPUT_DIR, f'temp/{x}/{y}'))
    
    sources = {
        'daily': daily_resource,
        'yearly': yearly
    }
    
    sinks = {
        'plant': plant,
        'soil': soil,
        'tempdir': tempdir
    }

    simplecrop(sources=sources, sinks=sinks)


daily = FeatherResource(os.path.join(BASE_DIR, 'simplecrop/data/daily.feather'))
yearly = FeatherResource(os.path.join(BASE_DIR, 'simplecrop/data/yearly.feather'))


with Flow('crop_pipeline') as flow:
    overland_flow = run_overland_flow()
    surface_water_depth_chunks = chunkify_soil_water_infiltration_depth(overland_flow)
    simplecrop_chunks = convert_swid_chunk_to_simplecrop.map(surface_water_depth_chunks)
    yield_chunks = simplecrop_process_chunk.map(simplecrop_chunks)

flow.run()
```
