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

Request
-------

Models can be run by issuing requests to them. A request contains metadata describing sources and sinks. A source describes the location of an existing file resource (web endpoints are also planned on being supported eventually). A sink describes the location of where you plan to write to (currently only a file).

Loaders
-------

Data must be accessed from a source in order to be used in a model. This could mean loading all the source data at once into memory or only loading chunks of the dataset into memory if the dataset is large. Meillionen has utility classes to make the process of loading data into commonly used datastructures (in Python Pandas or XArray are supported) less work for model developers.

Savers
------

Model results must be saved into sinks. Sink dataset saving may be done all at once for small datasets or in chunks for large datasets. Meillionen has helper classes for common Python packages like `pandas`, `netCDF4` and `xarray` here to make the process of saving to common formats simpler for model developers.