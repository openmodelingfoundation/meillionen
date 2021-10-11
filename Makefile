SHELL=/bin/bash
CONDA_ACTIVATE=source $$(conda info --base)/etc/profile.d/conda.sh ; conda activate ; conda activate

.PHONY: setup
setup: setup-conda setup-packages

.PHONY: setup-conda
setup-conda:
	conda env create -n meillionen -f python/environment.yml

.PHONY: setup-packages
setup-packages:
	$(CONDA_ACTIVATE) meillionen \
		&& cd python \
		&& poetry install --extras prefect \
		&& cd ../examples/crop-pipeline/simplecrop \
		&& poetry install \
		&& cd ../overlandflow \
		&& poetry install

.PHONY: build-docs
build-docs:
	cd docs && jupyter book build --warningiserror .

.PHONY: test
test: build-docs
	cd python && poetry run pytest
	cd examples/crop-pipeline/simplecrop && poetry run pytest
	cd examples/crop-pipeline/overlandflow && poetry run pytest
