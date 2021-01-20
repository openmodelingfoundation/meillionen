FROM rust:1.49

RUN apt-get update -y \
    && apt-get install -y python3-pip cmake \
    && pip3 install maturin numpy pandas \
    && apt-get clean autoclean \
    && apt-get autoremove --yes \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /tmp/*

COPY meillionen-mt /app/meillionen-mt
COPY meillionen-mt-derive /app/meillionen-mt-derive
COPY meillionen-store-netcdf /app/meillionen-store-netcdf
COPY simplecrop /app/simplecrop
COPY simplecrop-cli /app/simplecrop-cli

WORKDIR /app