PYTHON_PATH=.venv/bin/python
SIMPLECROP_BASE=examples/crop-pipeline/simplecrop/simplecrop
WHEELS=target/wheels

.PHONY: build
build:
	. .venv/bin/activate && maturin build -i $$(which python) --manifest-path meillionen-mt-python/Cargo.toml --cargo-extra-args='--features "pyo3/extension-module"'
	. .venv/bin/activate && maturin build -i $$(which python) --manifest-path examples/crop-pipeline/simplecrop/Cargo.toml --cargo-extra-args='--features "pyo3/extension-module"'
	$(PYTHON_PATH) -m pip uninstall -y meillionen-mt-python simplecrop_cli
	$(PYTHON_PATH) -m pip install --no-deps --force-reinstall -r requirements/wheels.txt

.PHONY: build-simplecrop
build-simplecrop: target/simplecrop/simplecrop
	docker-compose build

.PHONY: pull-images
	docker pull gcc jupyter/datascience-notebook

target/simplecrop/simplecrop: $(wildcard $(SIMPLECROP_BASE)/cli/*.f03) $(SIMPLECROP_BASE)/build.sh
	cd $(SIMPLECROP_BASE) \
		&& ./build.sh cli \
		&& cd - \
		&& mkdir -p target/simplecrop \
		&& mv $(SIMPLECROP_BASE)/target/cli/simplecrop $@
