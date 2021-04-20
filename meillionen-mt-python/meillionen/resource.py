"""
Resources are references to datasets that provide the information necessary to
load or save the data later.
"""

from meillionen.meillionen import SinkResource, SourceResource


def feather_sink(path: str) -> SinkResource:
    """
    Feather sink file

    :param path: local path to the file
    :return: A feather sink
    """
    return SinkResource.from_dict({
        'type': 'FeatherResource',
        'path': path,
    })


def file_sink(path) -> SinkResource:
    """
    File sink file

    A file sink is a catch all file sink type to support file types not
    directly supported by meillionen

    :param path: local path to the file
    :return: A file sink
    """
    return SinkResource.from_dict({
        'type': 'FileResource',
        'path': path,
    })


def netcdf_sink(path, variable) -> SinkResource:
    """
    NetCDF sink file

    :param path: local path to the file
    :param variable: variable name in NetCDF file
    :return: A NetCDF sink
    """
    return SinkResource.from_dict({
        'type': 'NetCDFResource',
        'path': path,
        'variable': variable,
        'data_type': 'Float32',
        'slices': {}
    })


def parquet_sink(path: str) -> SinkResource:
    """
    Parquet sink file

    :param path: local path to the file
    :return: A parquet sink
    """
    return SinkResource.from_dict({
        'type': 'ParquetResource',
        'path': path
    })


def feather_source(path: str) -> SourceResource:
    """
    Feather source file

    :param path: local path to the file
    :return: A feather source
    """
    return SourceResource.from_dict({
        'type': 'FeatherResource',
        'path': path,
    })


def file_source(path) -> SourceResource:
    """
    File source file

    A file source is a catch all file sink type to support file types not
    directly supported by meillionen

    :param path: local path to the file
    :return: A file source
    """
    return SourceResource.from_dict({
        'type': 'FileResource',
        'path': path,
    })


def netcdf_source(path: str, variable: str) -> SourceResource:
    """
    NetCDF variable in a source file

    :param path: local path to the file
    :param variable: variable name in NetCDF file
    :return: A netcdf source
    """
    return SourceResource.from_dict({
        'type': 'NetCDFResource',
        'path': path,
        'variable': variable,
        'data_type': 'Float32',
        'slices': {}
    })


def parquet_source(path: str) -> SourceResource:
    """
    Parquet source file

    :param path: local path to the file
    :return: A parquet source
    """
    return SourceResource.from_dict({
        'type': 'ParquetResource',
        'path': path,
    })