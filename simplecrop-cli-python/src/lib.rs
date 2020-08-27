use chrono::{DateTime, NaiveDateTime, Utc};
use pyo3::prelude::*;
use pyo3::exceptions;
use simplecrop_cli::{SimpleCropConfig, DailyData, YearlyData, SimpleCropDataSet};
use numpy::{PyArray1, ToPyArray, PyReadonlyArray1, IntoPyArray};
use pyo3::types::{PyDict, IntoPyDict};
use ndarray::Array1;
use meillionen_mt::{IntoPandas, FromPandas};
use meillionen_mt_derive::{IntoPandas, FromPandas};
use pyo3::callback::IntoPyCallbackOutput;

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

fn run(cli_path: String) -> PyResult<PySimpleCropDataSet> {
    // 365 days of weather
    let daily = DailyData {
        irrigation: ndarray::Array1::from_elem(1000, 0.0),
        energy_flux: ndarray::Array1::from_elem(1000, 5.1),
        temp_max: ndarray::Array1::from_elem(1000, 20.0),
        temp_min: ndarray::Array1::from_elem(1000, 4.4),
        rainfall: ndarray::Array1::from_elem(1000, 23.9),
        photosynthetic_energy_flux: ndarray::Array1::from_elem(1000, 10.7)
    };

    let yearly = YearlyData::default();

    let config = SimpleCropConfig {
        daily,
        yearly
    };

    let r = config.run(
        &cli_path,
    ".",
        )
        .map_err(|e| exceptions::IOError::py_err(e.to_string()))?;
    Ok(PySimpleCropDataSet { inner: r })
}

#[pymodule]
fn simplecrop_cli_python(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PySimpleCropDataSet>()?;

    #[pyfn(m, "run")]
    #[text_signature = "(cli_path, /)"]
    fn run_py(_py: Python, cli_path: String) -> PyResult<PySimpleCropDataSet> {
        run(cli_path)
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