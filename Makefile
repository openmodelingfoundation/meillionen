PYTHON_PATH=python
SIMPLECROP_BASE=examples/crop-pipeline/simplecrop/simplecrop
WHEELS=target/wheels

.PHONY: build
build:
	maturin build -i $$(which python) --target x86_64-unknown-linux-gnu --manifest-path meillionen-mt-python/Cargo.toml --cargo-extra-args='--features "pyo3/extension-module"'
	maturin build -i $$(which python) --target x86_64-unknown-linux-gnu --manylinux 2_24 --manifest-path examples/crop-pipeline/simplecrop/Cargo.toml --cargo-extra-args='--features "pyo3/extension-module"'
	$(PYTHON_PATH) -m pip uninstall -y meillionen simplecrop_omf
	$(PYTHON_PATH) -m pip install --no-deps --force-reinstall target/wheels/*.whl

