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

from meillionen.experiment import Experiment, PathSettings, Trial
from meillionen.handlers import NetCDFHandler, PandasHandler
from meillionen.resource import \
    FileResource, FeatherResource, NetCDFResource, ParquetResource
from prefect import task, Flow

os.environ['SIMPLECROP'] = 'simplecrop'
os.environ['RUST_BACKTRACE'] = '1'
BASE_DIR = '../../examples/crop-pipeline'


INPUT_DIR = os.path.join(BASE_DIR, 'workflows/inputs')
OUTPUT_DIR = os.path.join(BASE_DIR, 'workflows/outputs')

experiment = Experiment(
    sinks=PathSettings(base_path=OUTPUT_DIR),
    sources=PathSettings(base_path=INPUT_DIR)
)
trial = experiment.trial("2021-05-26")
```

and build a request to call our model with.

```{code-cell} ipython3
overlandflow = ClientFunctionModel.from_path(
    name='overlandflow', 
    path=os.path.join(BASE_DIR, 'overlandflow/model.py'),
    trial=trial
)
```

 We also need to create sources and sinks to describe our data

```{code-cell} ipython3
elevation = FileResource(".asc")

weather = FeatherResource()

sources = {
    'elevation': elevation,
    'weather': weather
}
```

Then call the overland flow model with our request (which will create files on the file system)

```{code-cell} ipython3
sinks = overlandflow.run(sources=sources)
```

```{code-cell} ipython3
sinks['soil_water_infiltration__depth'].to_dict()
```

## Running Simple Crop on its Own

`simplecrop`  is a model of yearly crop growth that operates on the command line. When called in the current directory with no arguments it expects that there is a data folder. It expects that the data folder contains five files

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
from meillionen.client import ClientFunctionModel
import pandas as pd

run_simple_crop = ClientFunctionModel.from_path(
    name='simplecrop', 
    path='simplecrop_omf',
    trial=trial
)
```

```{code-cell} ipython3
sources = {
    'daily': FeatherResource(),
    'yearly': FeatherResource()
}

sinks = {
    'plant': FeatherResource(),
    'soil': FeatherResource(),
    'tempdir': FileResource(ext="", name='tmp')
}
```

```{code-cell} ipython3
run_simple_crop.run(sources=sources, sinks=sinks)
```

```{code-cell} ipython3
soil_df = PandasHandler(run_simple_crop.sink('soil')).load(s['soil'])
soil_df
```

## Using Prefect to Feed Overland Flow Results into Simple Crop

```{code-cell} ipython3
from meillionen.client import ResourceBuilder
from pyarrow.dataset import partitioning
import pyarrow as pa
import pandas as pd
import pathlib

trial = experiment.trial("simplecrop-parallelism")

overlandflow_sources = {
  'elevation': FileResource(ext='.asc'),
  'weather': FeatherResource()
}

overlandflow = ClientFunctionModel.from_path(
  name='overlandflow', 
  path=os.path.join(BASE_DIR, 'overlandflow/model.py'),
  trial=trial)

simplecrop_sinks = {
  'tempdir': FileResource(ext="", name="tmp")
}

simplecrop_partitioning = partitioning(
    pa.schema([("x", pa.int32()), ("y", pa.int32())]))

simplecrop = ClientFunctionModel.from_path(
  name='simplecrop_omf', 
  path='simplecrop_omf',
  sinks=simplecrop_sinks,
  trial=trial,
  partitioning=simplecrop_partitioning)
```

```{code-cell} ipython3
class DailyRainfallCreator(ResourceBuilder):
    def __init__(self, settings, partitioning, validator):
        self.handler = PandasHandler(validator)
        super().__init__(name='daily', settings=settings, partitioning=partitioning)
        
    def save(self, daily_df, rainfall, partition):
        daily_df['rainfall'] = rainfall
        daily = self._complete(FeatherResource(), partition=partition)
        self.handler.save(daily, data=daily_df)
        return daily


daily_df = pd.read_feather(os.path.join(trial.sources.base_path, 'daily.feather'))
yearly = FeatherResource().build(settings=trial.sources, name='yearly')
        
daily_rainfall_creator = DailyRainfallCreator(
    settings=trial.sinks,
    partitioning=simplecrop_partitioning,
    validator=simplecrop.source('daily')
)

        
@task()
def run_overland_flow():
    sources = {
        'elevation': FileResource(ext=".asc"),
        'weather': FeatherResource()
    }

    sinks = overlandflow.run(sources=sources)

    return sinks['soil_water_infiltration__depth']


@task()
def chunkify_soil_water_infiltration_depth(swid):
    variable = NetCDFHandler(overlandflow.sink('soil_water_infiltration__depth')).load(swid)
    return [{'soil_water_infiltration__depth': variable[x, y, :], 'x': x, 'y': y}
            for (x,y) in itertools.product(range(10,12), range(21,23))]


@task()
def simplecrop_process_chunk(data):
    soil_water_infiltration__depth = data['soil_water_infiltration__depth']
    x, y = data['x'], data['y']
    daily = daily_rainfall_creator.save(
        daily_df,
        soil_water_infiltration__depth,
        partition=dict(x=x, y=y))
    
    sources = {
        'daily': daily,
        'yearly': yearly
    }

    sinks = simplecrop.run(sources=sources, partition=dict(x=x, y=y))

with Flow('crop_pipeline') as flow:
    overland_flow = run_overland_flow()
    surface_water_depth_chunks = chunkify_soil_water_infiltration_depth(overland_flow)
    yield_chunks = simplecrop_process_chunk.map(surface_water_depth_chunks)

flow.run()
```

```{code-cell} ipython3
from pyarrow.dataset import dataset

plant_df = dataset(os.path.join(OUTPUT_DIR, 'simplecrop-parallelism/plant'), partitioning=simplecrop_partitioning)
plant_df.to_table().to_pandas()
```
