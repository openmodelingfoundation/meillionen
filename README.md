# Meillionen

Meillionen will be a project to facilitate the interoperability of simulation models written in different languages and frameworks for the physical and social sciences. Currently it is being used to experiment with different ways of connecting the SimpleCrop CLI/library with [LandLab](https://landlab.github.io/#/) and making models conform to the [PyMT](https://pymt.readthedocs.io/en/latest/) interface.

## Testing

```bash
cargo test --manifest-path simplecrop-cli-python/Cargo.toml --no-default-features --features static        
```

## Building the Python library

See the README in the `simplecrop-cli-python` folder
