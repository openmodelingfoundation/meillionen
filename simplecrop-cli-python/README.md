# Python CLI wrapper for SimpleCrop

This crate builds a python library using maturin.

First you will need to set up a python environment for testing and building:

```bash
conda env install -f environment.yml
conda activate simplecrop-cli
```

Use

```
maturin build
```

to build a wheel for distribution.

Use

```
maturin develop
```

to try out compiled python library in the REPL

```python
import simplecrop_cli_python

# run the model in the current directory
simplecrop_cli_python.run("../simplecrop/target/cli/simplecrop", ".")
```

Tests in the project can be run with

```bash
cargo test --no-default-features --features static        
```
