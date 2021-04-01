# Meillionen

Meillionen will be a project to facilitate the interoperability of simulation models written in different languages and frameworks for the physical and social sciences. Currently it is being used to experiment with different ways of connecting the SimpleCrop CLI/library with [LandLab](https://landlab.github.io/#/) and making models conform to the [PyMT](https://pymt.readthedocs.io/en/latest/) interface.

## Setup

In order to setup this project you'll have to install `cargo` and the rust toolchain. You can install `cargo` through [rustup](https://rustup.rs/#) or [conda](https://anaconda.org/conda-forge/rust) (`conda install -c conda-forge rust` should do the trick).

Next you'll have to install the overall python dependencies

```bash
# create a python venv
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements/base.txt 
```

The `Makefile` in the project root can be used to build and install the `simplecrop` and `meillionen-mt-python`
packages. You can install both those packages with

```bash
make
```

Currently this is setup only for linux (`requirements/wheels.txt` assumes a linux operating system and python 3.8). With
templated wheel dependencies the make file should be cross platform.

## Build

```
cargo build
```

## Test

```bash
cargo test        
```

## Build the Python libraries

```bash
make build
```
