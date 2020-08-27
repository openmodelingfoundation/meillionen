use ndarray::Array1;
use numpy;
use numpy::IntoPyArray;
use std::sync::Arc;
use pyo3::types::PyDict;
use pyo3::{Python, PyResult, PyAny, PyErr};
use pyo3::prelude::PyModule;

pub trait IntoPandas: Sized {
    fn into_pandas(self, py: Python) -> PyResult<&PyAny>;
}

pub trait FromPandas: Sized {
    fn from_pandas(obj: &PyAny) -> Result<Self, PyErr>;
}

#[cfg(test)]
mod tests {
}