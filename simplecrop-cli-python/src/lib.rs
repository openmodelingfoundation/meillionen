use chrono::{DateTime, NaiveDateTime, Utc};
use pyo3::prelude::*;
use pyo3::exceptions;
use simplecrop_cli::{SimpleCropConfig, DailyData, YearlyData, SimpleCropDataSet};
use numpy::{PyArray1, ToPyArray, PyReadonlyArray1, IntoPyArray};
use pyo3::types::{PyDict, IntoPyDict};
use ndarray::Array1;
use meillionen_mt::{IntoPandas, FromPandas};
use meillionen_mt_derive::FromPandas;
use pyo3::callback::IntoPyCallbackOutput;

// #[pyclass]
// #[derive(Debug)]
// pub struct PySimpleCropDataSet { inner: SimpleCropDataSet }
//
// #[pymethods]
// impl PySimpleCropDataSet {
//     fn print(&self, _py: Python) {
//         print!("{:?}", self.inner)
//     }
// }

#[derive(FromPandas)]
pub struct Coords {
    xs: Array1<f64>,
    ys: Array1<i64>
}

impl IntoPandas for Coords {
    fn into_pandas(self, py: Python) -> PyResult<&PyAny> {
        let pandas = PyModule::import(py, "pandas")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("xs", self.xs.into_pyarray(py))?;
        kwargs.set_item("ys", self.ys.into_pyarray(py))?;
        pandas.call1("DataFrame", (kwargs,))
    }
}

// impl FromPandas for Coords {
//     type Error = PyErr;
//
//     fn from_pandas(obj: &PyAny) -> Result<Self, Self::Error> {
//         let xs = obj.get_item("xs").unwrap().extract::<&PyArray1<f64>>()?;
//         let ys = obj.get_item("ys").unwrap().extract::<&PyArray1<i64>>()?;
//         Ok(Self { xs: xs.to_owned_array(), ys: ys.to_owned_array() })
//     }
// }

// fn run(cli_path: String) -> PyResult<PySimpleCropDataSet> {
//     // 365 days of weather
//     let daily = DailyData {
//         irrigation: ndarray::Array1::from_elem(1000, 0.0),
//         energy_flux: ndarray::Array1::from_elem(1000, 5.1),
//         temp_max: ndarray::Array1::from_elem(1000, 20.0),
//         temp_min: ndarray::Array1::from_elem(1000, 4.4),
//         rainfall: ndarray::Array1::from_elem(1000, 23.9),
//         photosynthetic_energy_flux: ndarray::Array1::from_elem(1000, 10.7)
//     };
//
//     let yearly = YearlyData::default();
//
//     let config = SimpleCropConfig {
//         daily,
//         yearly
//     };
//
//     let r = config.run(
//         &cli_path,
//     ".",
//         )
//         .map_err(|e| exceptions::IOError::py_err(e.to_string()))?;
//     Ok(PySimpleCropDataSet { inner: r })
// }

#[pymodule]
fn simplecrop_cli_python(_py: Python, m: &PyModule) -> PyResult<()> {
    // m.add_class::<PySimpleCropDataSet>()?;

    #[pyfn(m, "run")]
    #[text_signature = "(cli_path, /)"]
    fn run_py(_py: Python, cli_path: String) -> PyResult<PySimpleCropDataSet> {
        run(cli_path)
    }

    #[pyfn(m, "to_dataframe")]
    #[text_signature = "(pydf)"]
    fn to_dataframe_py<'a>(_py: Python<'a>, pydf: &PyAny) -> PyResult<&'a PyAny> {
        // let class_name = pydf.
        //     getattr("__class__")?.
        //     getattr("__name__")?.
        //     extract::<&str>()?;
        // let (xs, ys) = if class_name == "DataFrame" {
        //     let xs = pydf
        //         .get_item("xs")?
        //         .call_method0("to_numpy")?
        //         .extract::<&PyArray1<f64>>()?;
        //     let ys = pydf
        //         .get_item("ys")?
        //         .call_method0("to_numpy")?
        //         .extract::<&PyArray1<i64>>()?;
        //     (xs, ys)
        // } else if class_name == "dict" {
        //     let xs = pydf
        //         .get_item("xs")?
        //         .extract::<&PyArray1<f64>>()?;
        //     let ys = pydf
        //         .get_item("ys")?
        //         .extract::<&PyArray1<i64>>()?;
        //     (xs, ys)
        // } else {
        //     Err(exceptions::TypeError::py_err("Argument needs to be a dict or a dataframe"))?
        // };
        //
        // let mut coords = Coords {
        //   xs: xs.to_owned_array(),
        //   ys: ys.to_owned_array()
        // };
        let mut coords = Coords::from_pandas(pydf)?;
        coords.xs = &coords.xs + &coords.xs;
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