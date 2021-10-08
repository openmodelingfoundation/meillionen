Introduction
============

Motivation
----------

### Using Existing Computational Models

The difficulties of combining computational models are many. Existing computational models are often incompletely documented, have no build system, are written in different programming languages, use a variety of model formalisms and have idiosyncratic ways of consuming and producing data. 

Interested parties needs to deal with those challenges in order to use someone else's model so if a model producer wants other people to use their model they need to alleviate those challenges. Model producers currently attempt to alleviate those challenges by providing complete documentation, having a clear one-step installation process or distributing built versions, using programming languages that either allow use as a shared library (C, C++, Fortran) or that are commonly used in the domain (Python, R), allowing variable time step sizes and using common data interface formats such as C compatible data types, Apache Arrow, NetCDF etcetera as well as clearly documenting the units of measure needed for each variable.

A shared library method of distributing a model or series of models is common for larger, core functionality projects with many contributors. Ease of use for the library requires wrapping it in a higher level language like Python as a package. This will likely requires more work than developing solely in a language with higher level features because we need to maintain a few wrappers and potentially support different build targets (x86_64 windows, ARM Linux etc) but having a consistent cross-language API makes it much easier for modeller's to try out a model to see if it meets their needs.

A package method of model distribution involves creating a language specific package that includes all related models. Use of a language's package tooling has a number of beneficial features including support for generating documentation, a structured project layout and ease of testing.

A service method of distributing a model or series of models allows interested users to run the model by accessing a web service API. This can be beneficial for hard to install models or models that require a large amount of data.

A command line method of distributing a model allows the model to be executed from the shell. This can be beneficial for use in scripts.

### Existing Model Integration Frameworks and Standards

- [Simulation Modelling Integration Framework (SMIF)](https://github.com/nismod/smif) is an existing framework that supports running system of system models. It supports running discrete time state system models through a web interface or on the command line. Models run by being executed in the shell. Data is passed between models using CSV, Parquet and vector format files. Data is validated using external schemas (written in YML).

- [OpenMI](https://www.ogc.org/standards/openmi) is a standard for coupling models. Implementations have adapters to deal with differing spatial, unit of measure and temporal extents and models communicate in memory through getters and setters.

- [BMI](https://bmi.readthedocs.io/en/latest/) is another model coupling framework. It specifies an interface where there are initialization and finalization methods as well as model variable getters and setters with [standard names](https://csdms.colorado.edu/wiki/CSDMS_Standard_Names). This modeling toolkit has stores that adopt the BMI getter and setter interface and can be backed by different backends such as NetCDF or memory.

### Modeling Data Interoperability Toolkit

Computational models for hydrology, ecology, social systems and others areas are written in a variety of frameworks and languages. The OMF intends to make the use of model written in other frameworks and languages easier by providing a cross-language data communication library. The data communication library aims to

1. Reduce the burden of coupling computational models written in different frameworks and languages for model consumers
2. Have a low barrier to entry for model developers to make their models couple easily with other standards adopting models
3. Support for basic metadata standards so that modellers can find models they're interested in.
4. Agnostic to execution strategy - allow for use in workflow manager for DAG jobs, support models with two-way coupling as well
5. Only deal with data access to make it possible to integrate with multiple frameworks. 
6. Support different deployment strategies - the model interface should support containerization as well as use as regular language installed package

The pre-alpha [meillionen](https://github.com/openmodelingfoundation/meillionen) library is in the early stages of attempting to address those goals when used alongside an existing modelling toolkit. Currently the library supports creating command line interface wrappers that give the model user schemas about what sort of data the model expected to consume and produce as well as a uniform way to call those models. In future versions we would like to support streaming of data to models as well as support for model communication using http 2. 
