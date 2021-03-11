FROM rust:1.49

RUN apt-get update -y \
    && apt-get install -y python3-pip cmake \
    && pip3 install maturin numpy pandas \
    && python3 -m pip install --upgrade pip \
    && apt-get clean autoclean \
    && apt-get autoremove --yes \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /tmp/*

COPY meillionen-mt /app/meillionen-mt
COPY meillionen-mt-derive /app/meillionen-mt-derive
COPY meillionen-store-netcdf /app/meillionen-store-netcdf
COPY simplecrop /app/simplecrop
COPY simplecrop-cli /app/simplecrop-cli

WORKDIR /app/simplecrop-cli

RUN maturin build \
    && python3 -m pip install target/wheels/simplecrop_cli-0.1.0-cp37-cp37m-manylinux2010_x86_64.whl

ENTRYPOINT ["./cli.py"]