FROM jupyter/datascience-notebook

COPY ./target/simplecrop/simplecrop $HOME/simplecrop
COPY \
  ./target/wheels/meillionen_mt_python-0.1.0-cp38-cp38-manylinux2010_x86_64.whl \
  ./target/wheels/simplecrop_cli-0.1.0-cp38-cp38-manylinux2010_x86_64.whl \
  /temp/
USER root
RUN pip install /temp/* && rm -rf /temp
USER $NB_UID
