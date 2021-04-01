use std::collections::BTreeMap;
use std::sync::Arc;

use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use std;

use pythonize::{depythonize, pythonize};
use serde::{Deserialize, Serialize};

use meillionen_mt::arg::req;
use meillionen_mt::model;
use meillionen_mt::{arg, extension_columns as ext_cols};

fn to_dict<T>(data: &T) -> PyResult<PyObject>
where
    T: Serialize,
{
    pythonize(Python::acquire_gil().python(), &data)
        .map_err(|e| PyValueError::new_err(format!("{:?}", e)))
}

fn from_dict<'de, T>(data: &'de PyAny) -> PyResult<T>
where
    T: Deserialize<'de>,
{
    depythonize(data).map_err(|e| PyValueError::new_err(format!("{:?}", e)))
}

#[pyclass]
#[derive(Debug)]
struct SourceResource {
    inner: req::SourceResource,
}

#[pymethods]
impl SourceResource {
    fn to_dict(&self) -> PyResult<PyObject> {
        to_dict(&self.inner)
    }
}

#[pyfunction]
fn netcdf_source(data: &PyAny) -> PyResult<SourceResource> {
    Ok(SourceResource {
        inner: from_dict(data)?,
    })
}

#[pyfunction]
fn feather_source(path: String) -> SourceResource {
    SourceResource {
        inner: Arc::new(req::FeatherResource { path }),
    }
}

#[pyfunction]
fn file_source(path: String) -> SourceResource {
    SourceResource {
        inner: Arc::new(req::FileResource { path }),
    }
}

#[pyfunction]
fn parquet_source(path: String) -> SourceResource {
    SourceResource {
        inner: Arc::new(req::ParquetResource { path }),
    }
}

#[pyclass]
#[derive(Debug)]
struct SinkResource {
    inner: req::SinkResource,
}

#[pymethods]
impl SinkResource {
    fn to_dict(&self) -> PyResult<PyObject> {
        to_dict(&self.inner)
    }
}

#[pyfunction]
fn netcdf_sink(data: &PyAny) -> PyResult<SinkResource> {
    Ok(SinkResource {
        inner: from_dict(data)?,
    })
}

#[pyfunction]
fn feather_sink(path: String) -> SinkResource {
    SinkResource {
        inner: Arc::new(req::FeatherResource { path }),
    }
}

#[pyfunction]
fn file_sink(path: String) -> SinkResource {
    SinkResource {
        inner: Arc::new(req::FileResource { path }),
    }
}

#[pyfunction]
fn parquet_sink(path: String) -> SinkResource {
    SinkResource {
        inner: Arc::new(req::ParquetResource { path }),
    }
}

type SinkResourceMap = BTreeMap<String, SinkResource>;
type SourceResourceMap = BTreeMap<String, SourceResource>;

#[pyclass]
#[derive(Debug)]
struct TensorValidator {
    inner: Arc<arg::validation::TensorValidator>,
}

#[pymethods]
impl TensorValidator {
    #[staticmethod]
    fn from_dict(data: &PyAny) -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(from_dict(data)?),
        })
    }

    fn to_dict(&self) -> PyResult<PyObject> {
        to_dict(&self.inner)
    }
}

#[pyclass]
#[derive(Debug)]
struct ArgValidatorType {
    inner: Arc<arg::ArgValidatorType>,
}

#[pymethods]
impl ArgValidatorType {
    #[staticmethod]
    fn from_dict(data: &PyAny) -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(from_dict(data)?),
        })
    }

    fn to_dict(&self) -> PyResult<PyObject> {
        to_dict(&self.inner)
    }
}

#[pyclass]
#[derive(Debug)]
struct DimMeta {
    inner: Arc<ext_cols::DimMeta>,
}

impl DimMeta {
    fn new(inner: Arc<ext_cols::DimMeta>) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl DimMeta {
    #[new]
    fn init(name: String, size: usize, description: Option<String>) -> Self {
        Self {
            inner: Arc::new(ext_cols::DimMeta {
                name,
                size,
                description,
            }),
        }
    }

    #[getter]
    fn name(&self) -> &str {
        self.inner.name.as_ref()
    }

    #[getter]
    fn size(&self) -> usize {
        self.inner.size
    }

    #[getter]
    fn description(&self) -> Option<&String> {
        self.inner.description.as_ref()
    }
}

#[pyclass]
#[derive(Debug)]
struct FuncRequest {
    inner: model::FuncRequest,
}

#[pymethods]
impl FuncRequest {
    #[new]
    pub fn new() -> Self {
        Self {
            inner: model::FuncRequest::new(),
        }
    }

    pub fn set_sink(&mut self, s: &str, si: &SinkResource) {
        self.inner.set_sink(s, &si.inner)
    }

    pub fn set_source(&mut self, s: &str, sr: &SourceResource) {
        self.inner.set_source(s, &sr.inner)
    }

    pub fn get_sink(&self, s: &str) -> Option<SinkResource> {
        self.inner.get_sink(s).map(|sr| SinkResource { inner: sr })
    }

    pub fn get_source(&self, s: &str) -> Option<SourceResource> {
        self.inner
            .get_source(s)
            .map(|sr| SourceResource { inner: sr })
    }

    pub fn to_dict(&self) -> PyResult<PyObject> {
        to_dict(&self.inner)
    }
}

#[pyclass]
#[derive(Debug)]
pub struct FuncInterface {
    inner: Arc<model::FuncInterface>,
}

impl FuncInterface {
    pub fn new(inner: Arc<model::FuncInterface>) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl FuncInterface {
    #[new]
    fn __init__(s: &str) -> Self {
        Self {
            inner: Arc::new(model::FuncInterface::new(s)),
        }
    }

    #[staticmethod]
    fn from_json(s: &str) -> PyResult<Self> {
        Ok(Self {
            inner: serde_json::from_str(s).map_err(|e| PyIOError::new_err(e.to_string()))?,
        })
    }

    #[staticmethod]
    fn from_dict(data: &PyAny) -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(from_dict(data)?),
        })
    }

    fn to_dict(&self) -> PyResult<PyObject> {
        to_dict(&self.inner)
    }

    fn to_cli(&self) -> FuncRequest {
        FuncRequest {
            inner: self.inner.to_cli(),
        }
    }

    fn call_cli(&self, program_name: &str, fc: &FuncRequest) -> PyResult<()> {
        let output = self
            .inner
            .call_cli(program_name, &fc.inner)
            .map_err(|err| PyIOError::new_err(err.to_string()))?;
        if output.status.success() {
            print!("{}", std::str::from_utf8(output.stdout.as_ref()).unwrap());
            Ok(())
        } else {
            Err(PyIOError::new_err(
                std::str::from_utf8(&output.stderr)
                    .unwrap_or("Unknown error")
                    .to_string(),
            ))
        }
    }
}

#[pymodule]
fn meillionen(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(pyo3::wrap_pyfunction!(netcdf_sink, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(feather_sink, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(file_sink, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(parquet_sink, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(netcdf_source, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(feather_source, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(file_source, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(parquet_source, m)?)?;

    m.add_class::<SinkResource>()?;
    m.add_class::<SourceResource>()?;
    m.add_class::<ArgValidatorType>()?;
    m.add_class::<FuncRequest>()?;
    m.add_class::<FuncInterface>()?;
    m.add_class::<DimMeta>()?;

    Ok(())
}
