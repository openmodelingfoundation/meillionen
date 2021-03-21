use std::convert::TryFrom;
use std::sync::Arc;

use pyo3::exceptions::{PyIOError, PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::PySequenceProtocol;

use meillionen_mt::{arg, extension_columns as ext_cols};
use meillionen_mt::arg::req;
use meillionen_mt::model;
use pyo3::types::PyDict;
use pythonize::{depythonize, pythonize};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

fn to_dict<T>(data: &T) -> PyResult<PyObject>
    where T: Serialize {
    pythonize(
        Python::acquire_gil().python(),
        &data
    ).map_err(|e| PyValueError::new_err(e.to_string()))
}

fn from_dict<'de, T>(data: &'de PyAny) -> PyResult<T>
    where T: Deserialize<'de> {
    depythonize(data).map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyclass]
#[derive(Debug)]
struct NetCDFResource {
    inner: Arc<req::NetCDFResource>,
}

#[pymethods]
impl NetCDFResource {
    #[staticmethod]
    pub fn from_dict(data: &PyAny) -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(from_dict(data)?)
        })
    }

    #[getter]
    fn get_path(&self) -> &str {
        self.inner.path.as_str()
    }

    #[getter]
    fn get_variable(&self) -> &str {
        self.inner.variable.as_str()
    }

    #[getter]
    fn get_slices(&self) -> HashMap<String, (usize, usize)> {
        self.inner.slices.clone()
    }
}

#[pyclass]
#[derive(Debug)]
struct FeatherResource {
    inner: Arc<req::FeatherResource>
}

#[pymethods]
impl FeatherResource {
    #[staticmethod]
    fn from_dict(data: &PyAny) -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(from_dict(data)?)
        })
    }

    #[getter]
    fn get_path(&self) -> &str {
        self.inner.path.as_ref()
    }
}

#[pyclass]
#[derive(Debug)]
struct ArgResource {
    inner: Arc<arg::ArgResource>
}

#[pymethods]
impl ArgResource {
    #[staticmethod]
    fn from_dict(data: &PyAny) -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(from_dict(data)?)
        })
    }

    fn to_dict(&self) -> PyResult<PyObject> {
        to_dict(&self.inner)
    }
}

#[pyclass]
#[derive(Debug)]
struct TensorValidator {
    inner: Arc<arg::validation::TensorValidator>
}

#[pymethods]
impl TensorValidator {
    #[staticmethod]
    fn from_dict(data: &PyAny) -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(from_dict(data)?)
        })
    }

    fn to_dict(&self) -> PyResult<PyObject> {
        to_dict(&self.inner)
    }
}

#[pyclass]
#[derive(Debug)]
struct ArgValidatorType {
    inner: Arc<arg::ArgValidatorType>
}

#[pymethods]
impl ArgValidatorType {
    #[staticmethod]
    fn from_dict(data: &PyAny) -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(from_dict(data)?)
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

    pub fn set_sink(&mut self, s: &str, si: &ArgResource) {
        self.inner.set_sink(s, si.inner.clone())
    }

    pub fn set_source(&mut self, s: &str, sr: &ArgResource) {
        self.inner.set_source(s, sr.inner.clone())
    }

    pub fn get_sink(&self, s: &str) -> Option<ArgResource> {
        self.inner
            .get_sink(s)
            .map(|sr| ArgResource { inner: sr.clone() })
    }

    pub fn get_source(&self, s: &str) -> Option<ArgResource> {
        self.inner
            .get_source(s)
            .map(|sr| ArgResource { inner: sr.clone() })
    }

    pub fn to_dict(&self) -> PyResult<PyObject> {
        to_dict(&self.inner)
    }
}

#[pyclass]
#[derive(Debug)]
struct FuncInterface {
    inner: Arc<model::FuncInterface>,
}

#[pymethods]
impl FuncInterface {
    #[new]
    fn new(s: &str) -> Self {
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
            inner: Arc::new(from_dict(data)?)
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

    fn call_cli(&self, program_name: &str, fc: &FuncRequest) -> PyResult<Option<i32>> {
        self.inner
            .call_cli(program_name, &fc.inner)
            .map(|ec| ec.code())
            .map_err(|err| PyIOError::new_err(err.to_string()))
    }
}

#[pymodule]
fn meillionen(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<NetCDFResource>()?;
    m.add_class::<FeatherResource>()?;
    m.add_class::<ArgResource>()?;
    m.add_class::<ArgValidatorType>()?;
    m.add_class::<FuncRequest>()?;
    m.add_class::<FuncInterface>()?;
    m.add_class::<DimMeta>()?;

    Ok(())
}
