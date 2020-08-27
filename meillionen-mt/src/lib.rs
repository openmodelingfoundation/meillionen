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

// pub struct DataFrame<'a> {
//     data: &'a [(&'a str, Box<dyn IntoPyArray>)]
// }
//
// impl<'a> DataFrame<'a> {
//     fn into_pandas(self, py: Python) -> PyResult<&PyAny> {
//         let mut dict = PyDict::new(py);
//         for (fieldname, arr) in self.data.into_iter() {
//             dict.set_item(fieldname, arr.into_pyarray())
//         }
//         let pandas = PyModule::import(py, "pandas")?;
//         pandas.call1("DataFrame", (dict,))
//     }
// }

#[cfg(test)]
mod tests {
}