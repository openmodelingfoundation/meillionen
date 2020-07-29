# Python CLI wrapper for SimpleCrop

This crate builds a python library using maturin.

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
