Concepts
========

The meillionen package includes support

Model Interface
---------------

All models in meillionen currently have a function interface definition. Function interface definitions describe what input (source) data the model expects and what output (sink) data the model produces. A model's interface can be accessed by invoking the model on the command line with the interface subcommand.

```bash
<model> interface
```

Invocation of the interface subcommand returns json that is interpreted by other programs with the meillionen libraries to facilitate data validation of inputs. For dataframe like data this means describing what names and data types columns in the dataframe should have. For tensor like data information on what dimension labels the tensor has as well as the tensor's element type.

Settings
--------

A model has settings. Settings provide conventions about where to save file resources to, provide default resource types
for method arguments and where to keep and runtime errors.

```python
import pyarrow as pa
from meillionen.client import Experiment

partition = Partition(pa.schema([pa.field('x', pa.float32(), nullable=False), pa.field('y', pa.float32(), nullable=False)]))
exp = Experiment.from_cli('simplecrop', partition=partition)
exp.run('run', partition=[('x', 3), ('y', 5)], kwargs={'yearly': Feather('yearly.feather'), 'daily': Feather('daily.feather')})
```

Resources
---------

A resource describes the location of data and how to access it.

Resources can be created interactively

```python
from meillionen.resource import Feather

pr = Feather('foo.feather')
```

Often partially applied versions are used so that settings can provide conventions about where to save file resources to.

```python
from meillionen.resource import FeatherPartial

fp = FeatherPartial()
```

Creation of custom resource types requires you to implement the resource interface. This means creating a python class with

1. a static name property. The name should be prefixed by the package it is part so if you were to add a resource type to the `meillionen` package the name should look like `meilionen::resource::<resource name>`
2. a `deserialize` class method that takes a python buffer and returns the class
3. a `serialize` method. The serialize method takes a flatbuffer buffer builder.

Schemas
-------

A schema describes the shape of data.

Request
-------

Models can be run by issuing requests to them. A request contains metadata describing sources and sinks. A source describes the location of an existing file resource (web endpoints are also planned on being supported eventually). A sink describes the location of where you plan to write to (currently only a file).

Loaders
-------

Data must be accessed from a source in order to be used in a model. This could mean loading all the source data at once into memory or only loading chunks of the dataset into memory if the dataset is large. Meillionen has utility classes to make the process of loading data into commonly used datastructures (in Python Pandas or XArray are supported) less work for model developers.

Savers
------

Model results must be saved into sinks. Sink dataset saving may be done all at once for small datasets or in chunks for large datasets. Meillionen has helper classes for common Python packages like `pandas`, `netCDF4` and `xarray` here to make the process of saving to common formats simpler for model developers.

Handlers can be created and used interactively

```python
import pyarrow as pa
from meillionen.handlers import PandasHandler
from meillionen.interface.resource import Feather

ph = PandasHandler(
    name='soil',
    s=pa.schema([
        pa.field('day_of_year', pa.int32(), nullable=False),
        pa.field('acidity', pa.float32(), nullable=True)
    ],
    mutable=Mutability.read)
)
f = Feather('foo.feather')
df = ph.load(f)
```

Custom handlers can also be created. In order to create a custom handler you must conform to the handler interface.

1. `from_kwargs` construct the handler given a description and schema
2. `serialize` write the class to a flatbuffer buffer builder
3. `load` load a resource
4. `save` save a dataset using the information provided by a resource