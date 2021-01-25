use meillionen_mt::model::{FuncInterface, StoreRef};
use pyo3::prelude::*;
use serde_json;
use pyo3::exceptions::PyIOError;
use std::collections::HashMap;

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

    fn to_cli(&self) -> HashMap<String, String> {
        self.inner.to_cli()
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
    m.add_class::<PyFuncInterface>()?;
    m.add_class::<PyStoreRef>()?;

    Ok(())
}
