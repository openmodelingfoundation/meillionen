use meillionen_mt::model;
use pyo3::prelude::*;

use pyo3::exceptions::{PyIOError, PyKeyError, PyValueError};

use meillionen_mt::extension_columns as ext_cols;

use pyo3::PySequenceProtocol;
use std::sync::Arc;

use std::convert::TryFrom;

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
struct TensorStackMeta {
    inner: Arc<ext_cols::TensorStackMeta>,
}

impl TensorStackMeta {
    fn new(inner: Arc<ext_cols::TensorStackMeta>) -> Self {
        Self { inner }
    }
}

#[pyproto]
impl PySequenceProtocol for TensorStackMeta {
    fn __len__(&self) -> usize {
        self.inner.dimensions().len()
    }

    fn __getitem__(&self, idx: isize) -> PyResult<DimMeta> {
        let idx: usize = TryFrom::try_from(idx)
            .map_err(|_e| PyValueError::new_err(format!("{} is out of bounds", idx)))?;
        let dim_meta = self
            .inner
            .dimensions()
            .get(idx)
            .ok_or(PyKeyError::new_err(format!("{} is out of bounds", idx)))?;
        Ok(DimMeta::new(dim_meta.clone()))
    }
}

#[pyclass]
#[derive(Debug)]
struct TableMeta {
    inner: ext_cols::TableMeta,
}

#[pymethods]
impl TableMeta {
    #[new]
    pub fn new() -> Self {
        Self {
            inner: ext_cols::TableMeta::TensorStackMeta(Arc::new(ext_cols::TensorStackMeta::new(
                vec![],
            ))),
        }
    }

    #[staticmethod]
    pub fn from_json(s: &str) -> PyResult<Self> {
        Ok(Self {
            inner: serde_json::from_str(s).map_err(|e| PyValueError::new_err(e.to_string()))?,
        })
    }

    pub fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner).map_err(|e| PyValueError::new_err(e.to_string()))
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

    pub fn set_sink(&mut self, s: &str, si: &StoreRef) {
        self.inner.set_sink(s, &si.inner)
    }

    pub fn set_source(&mut self, s: &str, sr: &StoreRef) {
        self.inner.set_source(s, &sr.inner)
    }

    pub fn get_sink(&self, s: &str) -> Option<StoreRef> {
        self.inner
            .get_sink(s)
            .map(|sr| StoreRef { inner: sr.clone() })
    }

    pub fn get_source(&self, s: &str) -> Option<StoreRef> {
        self.inner
            .get_source(s)
            .map(|sr| StoreRef { inner: sr.clone() })
    }
}

#[pyclass]
#[derive(Debug)]
struct FuncInterface {
    inner: model::FuncInterface,
}

#[pymethods]
impl FuncInterface {
    #[new]
    fn new(s: &str) -> Self {
        Self {
            inner: model::FuncInterface::new(s),
        }
    }

    #[staticmethod]
    fn from_json(s: &str) -> PyResult<Self> {
        Ok(Self {
            inner: serde_json::from_str(s).map_err(|e| PyIOError::new_err(e.to_string()))?,
        })
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

    fn print(&self) {
        println!("{:?}", self)
    }
}

#[pyclass]
#[derive(Debug)]
struct StoreRef {
    inner: model::StoreRef,
}

#[pymethods]
impl StoreRef {
    #[staticmethod]
    fn new_local_path(s: &str) -> Self {
        Self {
            inner: model::StoreRef::SimplePath(s.to_string()),
        }
    }

    fn extract_local_path(&self) -> Option<String> {
        if let model::StoreRef::SimplePath(path) = &self.inner {
            return Some(path.to_string());
        }
        None
    }
}

#[pyfunction]
fn get_dataframe_formats() -> Vec<&'static str> {
    vec!["Parquet"]
}

#[pyfunction]
fn get_tensor_formats() -> Vec<&'static str> {
    vec!["NetCDF"]
}

#[pymodule]
fn meillionen(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<FuncRequest>()?;
    m.add_class::<FuncInterface>()?;
    m.add_class::<StoreRef>()?;
    m.add_class::<TableMeta>()?;
    m.add_class::<TensorStackMeta>()?;
    m.add_class::<DimMeta>()?;
    m.add_function(pyo3::wrap_pyfunction!(get_tensor_formats, m)?)?;

    Ok(())
}
