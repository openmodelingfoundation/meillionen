# Meillionen

Meillionen will be a project to facilitate the interoperability of simulation models written in different languages and frameworks for the physical and social sciences. Current work is focused on combining models with command line interfaces in python but work on bidirectional communication is planned.

## Installation

With pip

```bash
pip install meillionen
```

## Development

Setup the environment

```bash
conda env create -f environment.yml -n meillionen
conda activate meillionen
```

Run the tests

```bash
poetry run pytest
```

Install the dependencies

```bash
poetry install
```
