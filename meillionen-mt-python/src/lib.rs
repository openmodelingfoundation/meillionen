use meillionen_mt::model::{FuncInterface, StoreRef, FuncRequest};
use pyo3::prelude::*;
use serde_json;
use pyo3::exceptions::PyIOError;
use std::collections::HashMap;

#[pyclass]
#[derive(Debug)]
struct PyFuncRequest {
    inner: FuncRequest
}

#[pymethods]
impl PyFuncRequest {
    pub fn get_source(&self, s: &str) -> Option<PyStoreRef> {
        self.inner.get_source(s).map(|sr| PyStoreRef { inner: sr.clone() })
    }

    pub fn get_sink(&self, s: &str) -> Option<PyStoreRef> {
        self.inner.get_sink(s).map(|sr| PyStoreRef { inner: sr.clone() })
    }
}

#[pyclass]
#[derive(Debug)]
struct PyFuncInterface { inner: FuncInterface }

#[pymethods]
impl PyFuncInterface {
    #[new]
    fn new(s: &str) -> Self {
        Self {
            inner: FuncInterface::new(s)
        }
    }

    #[staticmethod]
    fn from_json(s: &str) -> PyResult<Self> {
        Ok(Self {
            inner: serde_json::from_str(s).map_err(|e|
                PyErr::from(PyIOError::new_err(e.to_string())))?
        })
    }

    fn to_cli(&self) -> PyFuncRequest {
        PyFuncRequest {
            inner: self.inner.to_cli()
        }
    }

    fn call_cli(&self, program_name: &str, fc: &PyFuncRequest) -> PyResult<Option<i32>> {
        self.inner.call_cli(program_name, &fc.inner)
            .map(|ec| ec.code())
            .map_err(|err| PyErr::from(PyIOError::new_err(err.to_string())))
    }

    fn print(&self) { println!("{:?}", self) }
}

#[pyclass]
#[derive(Debug)]
struct PyStoreRef { inner: StoreRef }

#[pymethods]
impl PyStoreRef {
    #[staticmethod]
    fn new_local_path(s: &str) -> Self {
        Self {
            inner: StoreRef::LocalPath(s.to_string())
        }
    }

    fn extract_local_path(&self) -> Option<String> {
        if let StoreRef::LocalPath(path) = &self.inner {
            return Some(path.to_string())
        }
        None
    }
}

#[pymodule]
fn meillionen(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyFuncRequest>()?;
    m.add_class::<PyFuncInterface>()?;
    m.add_class::<PyStoreRef>()?;

    Ok(())
}
