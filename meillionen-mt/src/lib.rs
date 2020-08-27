use ndarray::Array1;
use numpy;
use numpy::IntoPyArray;
use std::sync::Arc;

pub trait IntoPandas: Sized {
    fn into_pandas(self, py: pyo3::Python) -> pyo3::PyResult<&pyo3::types::PyAny>;
}

pub trait FromPandas: Sized {
    fn from_pandas(obj: &pyo3::types::PyAny) -> Result<Self, pyo3::PyErr>;
}

#[cfg(test)]
mod tests {
}