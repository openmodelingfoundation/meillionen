# Meillionen

Meillionen will be a project to facilitate the interoperability of simulation models written in different languages and frameworks for the physical and social sciences. Currently it is being used to experiment with different ways of connecting the SimpleCrop CLI/library with [LandLab](https://landlab.github.io/#/) and making models conform to the [PyMT](https://pymt.readthedocs.io/en/latest/) interface.

## Setup

In order to setup this project you'll have to install `conda`.

Then run

```bash
make setup
```

to setup the conda env and install the needed packages

## Examples

Example models and workflows are in the `examples` directory

## Documentation

To build the documentation you'll need a `meillionen` python development environment setup (see `python/README.md` for details) which will result in the `ghp-import` package being installed.

### Instructions

Go into the docs folder

```bash
cd docs
```

Then build the jupyter book docs

```bash
jupyter book build .
```

Push those docs to the `gh-pages` branch on github when the built docs look ready

```build
ghp-import --no-jekyll -o _build/html -p
```
