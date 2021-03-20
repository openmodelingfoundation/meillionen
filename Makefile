PYTHON_PATH=.venv/bin/python


.PHONY: build
build:
	. .venv/bin/activate && maturin build -i $$(which python) --manifest-path meillionen-mt-python/Cargo.toml --cargo-extra-args='--features "pyo3/extension-module"'
	. .venv/bin/activate && maturin build -i $$(which python) --manifest-path examples/crop-pipeline/simplecrop/Cargo.toml --cargo-extra-args='--features "pyo3/extension-module"'
	$(PYTHON_PATH) -m pip uninstall -y meillionen-mt-python simplecrop_cli
	$(PYTHON_PATH) -m pip install --no-deps --force-reinstall -r requirements/wheels.txt
