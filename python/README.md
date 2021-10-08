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

### FAQ

*I've updated the flatbuffer schemas, how do I update the generated Python code?*

Run `flatc` in the root of the project (it is installed in the flatbuffers conda package).

```bash
flatc -o python --python schema/*.fbs
```
