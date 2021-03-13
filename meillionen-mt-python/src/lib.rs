use meillionen_mt::model;
use pyo3::prelude::*;
use serde_json;
use pyo3::exceptions::{PyIOError, PyTypeError, PyValueError};
use std::collections::HashMap;
use meillionen_mt::extension_columns as ext_cols;
use std::borrow::Borrow;
use std::sync::Arc;

#[pyclass]
#[derive(Debug)]
struct TensorStackMeta {
    inner: Arc<ext_cols::TensorStackMeta>
}

impl TensorStackMeta {
    fn new(inner: Arc<ext_cols::TensorStackMeta>) -> Self {
        Self {
            inner
        }
    }
}

#[pyclass]
#[derive(Debug)]
struct TableMeta {
    inner: ext_cols::TableMeta
}

#[pymethods]
impl TableMeta {
    #[staticmethod]
    pub fn from_json(s: &str) -> PyResult<Self> {
        Ok(Self {
            inner: serde_json::from_str(s).map_err(|e| PyErr::from(PyValueError::new_err(e.to_string())))?
        })
    }

    pub fn get_tensor_stack_meta(&self) -> TensorStackMeta {
        match &self.inner {
            ext_cols::TableMeta::TensorStackMeta(ref tm) => TensorStackMeta::new(tm.clone()),
        }
    }
}

#[pyclass]
#[derive(Debug)]
struct FuncRequest {
    inner: model::FuncRequest
}

#[pymethods]
impl FuncRequest {
    #[new]
    pub fn new() -> Self {
        Self {
            inner: model::FuncRequest::new()
        }
    }

    pub fn set_sink(&mut self, s: &str, si: &StoreRef) {
        self.inner.set_sink(s, &si.inner)
    }

    pub fn set_source(&mut self, s: &str, sr: &StoreRef) {
        self.inner.set_source(s, &sr.inner)
    }

    pub fn get_sink(&self, s: &str) -> Option<StoreRef> {
        self.inner.get_sink(s).map(|sr| StoreRef { inner: sr.clone() })
    }

    pub fn get_source(&self, s: &str) -> Option<StoreRef> {
        self.inner.get_source(s).map(|sr| StoreRef { inner: sr.clone() })
    }
}

#[pyclass]
#[derive(Debug)]
struct FuncInterface { inner: model::FuncInterface }

#[pymethods]
impl FuncInterface {
    #[new]
    fn new(s: &str) -> Self {
        Self {
            inner: model::FuncInterface::new(s)
        }
    }

    #[staticmethod]
    fn from_json(s: &str) -> PyResult<Self> {
        Ok(Self {
            inner: serde_json::from_str(s).map_err(|e|
                PyErr::from(PyIOError::new_err(e.to_string())))?
        })
    }

    fn to_cli(&self) -> FuncRequest {
        FuncRequest {
            inner: self.inner.to_cli()
        }
    }

    fn call_cli(&self, program_name: &str, fc: &FuncRequest) -> PyResult<Option<i32>> {
        self.inner.call_cli(program_name, &fc.inner)
            .map(|ec| ec.code())
            .map_err(|err| PyErr::from(PyIOError::new_err(err.to_string())))
    }

    fn print(&self) { println!("{:?}", self) }
}

#[pyclass]
#[derive(Debug)]
struct StoreRef { inner: model::StoreRef }

#[pymethods]
impl StoreRef {
    #[staticmethod]
    fn new_local_path(s: &str) -> Self {
        Self {
            inner: model::StoreRef::LocalPath(s.to_string())
        }
    }

    fn extract_local_path(&self) -> Option<String> {
        if let model::StoreRef::LocalPath(path) = &self.inner {
            return Some(path.to_string())
        }
        None
    }
}

#[pymodule]
fn meillionen(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<FuncRequest>()?;
    m.add_class::<FuncInterface>()?;
    m.add_class::<StoreRef>()?;

    Ok(())
}
