mod array;
use std::sync::Arc;

use indoc::formatdoc;
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use std;

use pythonize::{depythonize, pythonize};
use serde::{Deserialize, Serialize};

use meillionen_mt::arg::req;
use meillionen_mt::model;
use meillionen_mt::arg;

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
    #[staticmethod]
    fn from_dict(data: &PyAny) -> PyResult<Self> {
        Ok(Self {
            inner: from_dict(data)?,
        })
    }

    fn to_dict(&self) -> PyResult<PyObject> {
        to_dict(&self.inner)
    }
}

#[pyclass]
#[derive(Debug)]
struct SinkResource {
    inner: req::SinkResource,
}

#[pymethods]
impl SinkResource {
    #[staticmethod]
    fn from_dict(data: &PyAny) -> PyResult<Self> {
        Ok(Self {
            inner: from_dict(data)?,
        })
    }

    fn to_dict(&self) -> PyResult<PyObject> {
        to_dict(&self.inner)
    }
}

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

    fn call_cli(&self, program_name: &str, fc: &FuncRequest) -> PyResult<String> {
        let output = self
            .inner
            .call_cli(program_name, &fc.inner)
            .map_err(|err| PyIOError::new_err(format!("{:?}", err)))?;
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            let out = String::from_utf8_lossy(&output.stdout);
            let err = String::from_utf8_lossy(&output.stderr);
            Err(PyIOError::new_err(formatdoc! {"

                Stdout:
                {}

                Stderr:
                {}",
                out,
                err
            }))
        }
    }
}

#[pyfunction]
fn create_interface_from_cli(path: &str) -> PyResult<FuncInterface> {
    Ok(FuncInterface::new(Arc::new(
        model::FuncInterface::from_cli(path).map_err(|e| PyIOError::new_err(format!("{:?}", e)))?,
    )))
}

#[pymodule]
fn meillionen(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(pyo3::wrap_pyfunction!(create_interface_from_cli, m)?)?;

    m.add_class::<SinkResource>()?;
    m.add_class::<SourceResource>()?;
    m.add_class::<ArgValidatorType>()?;
    m.add_class::<FuncRequest>()?;
    m.add_class::<FuncInterface>()?;

    array::init(m)?;

    Ok(())
}
