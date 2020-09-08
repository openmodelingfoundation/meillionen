mod model;
mod data;

use chrono::{DateTime, NaiveDateTime, Utc};
use pyo3::prelude::*;
use pyo3::exceptions;
use model::{SimpleCropConfig, DailyData, YearlyData, SimpleCropDataSet};
use numpy::{PyArray1, ToPyArray, PyReadonlyArray1, IntoPyArray};
use pyo3::types::{PyDict, IntoPyDict};
use ndarray::Array1;
use meillionen_mt::{IntoPandas, FromPandas};
use meillionen_mt_derive::{IntoPandas, FromPandas};

#[pyclass]
#[derive(Debug)]
pub struct PySimpleCropDataSet { inner: SimpleCropDataSet }

#[pymethods]
impl PySimpleCropDataSet {
    fn print(&self, _py: Python) {
        print!("{:?}", self.inner)
    }
}

#[derive(IntoPandas, FromPandas)]
pub struct Coords {
    xs: Array1<f64>,
    ys: Array1<i64>
}

fn run<'a>(py: Python<'a>, cli_path: String, daily_data: &PyAny) -> PyResult<&'a PyAny> {
    // 365 days of weather
    let daily = DailyData::from_pandas(daily_data)?;

    let yearly = YearlyData::default();

    let config = SimpleCropConfig {
        daily,
        yearly
    };

    let results = config.run(
        &cli_path,
    ".",
        )
        .map_err(|e| exceptions::IOError::py_err(e.to_string()))?;
    results.into_python(py)
}

#[pymodule]
fn simplecrop_cli_python(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PySimpleCropDataSet>()?;

    #[pyfn(m, "run")]
    #[text_signature = "(cli_path, daily_data)"]
    fn run_py<'a>(_py: Python<'a>, cli_path: String, daily_data: &PyAny) -> PyResult<&'a PyAny> {
        run(_py, cli_path, daily_data)
    }

    #[pyfn(m, "to_dataframe")]
    #[text_signature = "(pydf)"]
    fn to_dataframe_py<'a>(_py: Python<'a>, pydf: &PyAny) -> PyResult<&'a PyAny> {
        let mut coords = Coords::from_pandas(pydf)?;
        coords.xs = &coords.xs + &coords.xs;
        coords.ys = &coords.ys * 3;
        coords.into_pandas(_py)
    }

    #[pyfn(m, "to_df")]
    fn to_df_py<'a>(_py: Python<'a>) -> PyResult<&'a PyAny> {
        let mut coords = Coords {
          xs: Array1::from(vec![1.5,4.0,5.0,6.0]),
          ys: Array1::from(vec![3,5,6,2])
        };
        coords.into_pandas(_py)
    }

    Ok(())
}