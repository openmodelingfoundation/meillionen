Concepts
========

The meillionen package includes support

Model Interface
---------------

All models in meillionen currently have a class interface definition. Class interface definitions describe methods. Methods have arguments. Arguments (which are schemas) describe the shape of data as well as whether or not it is an input (`Mutability.read`) or an output (`Mutability.write`). A model's interface can be accessed by invoking the model on the command line with the interface subcommand.

```bash
<model> interface
```

Invocation of the interface subcommand returns flatbuffer that is interpreted by other programs with meillionen libraries to facilitate calling the model. For dataframe like data this means describing what names and data types columns in the dataframe should have. For tensor like data information on what dimension labels the tensor has as well as the tensor's element type.

Settings
--------

A model has settings. Settings provide conventions about where to save file resources to..

```python
import pyarrow as pa
from meillionen.settings import Settings

settings = Settings(base_path='output')
```

Resources
---------

A resource describes the location of data and how to access it.

Resources can be created interactively

```python
from meillionen.interface.resource import Feather

pr = Feather('foo.feather')
```

Often partially applied versions are used so that settings can provide conventions about where to save file resources to.

```python
from meillionen.interface.resource import Feather

fp = Feather.partial()
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

Models can be run by issuing requests to them. A request contains arguments for each argument specified in the `MethodInterface`.

Handlers
-------

Data handlers handle the loading and saving of data. Handlers also contain metadata about whether the resource is being writen to or read from. Data loading could mean loading all the source data at once into memory or only loading chunks of the dataset into memory if the dataset is large. Meillionen has utility classes to make the process of loading data into commonly used datastructures (in Python Pandas or XArray are supported) less work for model developers. Dataset saving may be done all at once for small datasets or in chunks for large datasets. Meillionen has helper classes for common Python packages like `pandas`, `netCDF4` and `xarray` here to make the process of saving to common formats simpler for model developers.

Handlers can be created and used interactively

```python
import pyarrow as pa
from meillionen.interface.schema import PandasHandler
from meillionen.interface.resource import Feather
from meillionen.interface.mutability import Mutability

ph = PandasHandler(
    name='soil',
    s=pa.schema([
        pa.field('day_of_year', pa.int32(), nullable=False),
        pa.field('acidity', pa.float32(), nullable=True)
    ]),
    mutability=Mutability.read
)
f = Feather('foo.feather')
df = ph.load(f)
```

Custom handlers can also be created. In order to create a custom handler you must conform to the handler interface.

1. `from_kwargs` construct the handler given a description and schema
2. `serialize` write the class to a flatbuffer buffer builder
3. `load` load a resource
4. `save` save a dataset using the information provided by a resource
