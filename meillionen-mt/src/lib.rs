use ndarray::{Array1, ArrayD};
use numpy;
use numpy::IntoPyArray;
use std::sync::Arc;
use pyo3::prelude::*;
use std::path::Iter;
use std::marker::PhantomData;

pub trait IntoPandas: Sized {
    fn into_pandas(self, py: pyo3::Python) -> pyo3::PyResult<&pyo3::types::PyAny>;
}

pub trait FromPandas: Sized {
    fn from_pandas(obj: &pyo3::types::PyAny) -> Result<Self, pyo3::PyErr>;
}

pub trait StoreVariable<T> {
    fn name(&self) -> String;
    fn slice(&self, indices: &[usize], slice_len: &[usize], strides: &[isize]) -> Array1<T>;
    fn get_dimensions(&self) -> Vec<String>;
}

#[cfg(test)]
mod tests {
}