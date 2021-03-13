.PHONY: build
build:
	cd meillionen-mt-python && maturin build
	cd examples/crop-pipeline/simplecrop && maturin build -i $$(which python)
	.venv/bin/python -m pip install --no-deps --force-reinstall -r requirements/wheels.txt
