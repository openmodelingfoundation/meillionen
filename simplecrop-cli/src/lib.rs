mod model;
mod data;


use clap::{App, Arg, SubCommand};
use pyo3::prelude::*;
use pyo3::exceptions;
use model::{SimpleCropConfig, DailyData, YearlyData, SimpleCropDataSet};

use ndarray::Array1;
use meillionen_mt::{IntoPandas, FromPandas};
use meillionen_mt::model::{FuncInterface};
use meillionen_mt_derive::{IntoPandas, FromPandas};
use crate::data::{F64CDFVariableRef, CDFStore};
use crate::model::SimpleCrop;
use std::env;
use itertools::Itertools;
use std::ffi::{OsString};

use std::collections::BTreeMap;
use pyo3::exceptions::PyIOError;
use pyo3::types::IntoPyDict;

#[pyclass]
#[derive(Debug)]
pub struct PySimpleCropDataSet { inner: SimpleCropDataSet }

#[pymethods]
impl PySimpleCropDataSet {
    fn print(&self, _py: Python) {
        print!("{:?}", self.inner)
    }
}

fn run(_py: Python, cli_path: String, daily_data: &PyAny) -> PyResult<()> {
    // 365 days of weather
    let daily = DailyData::from_pandas(daily_data)?;

    let yearly = YearlyData::default();

    let config = SimpleCropConfig {
        daily,
        yearly
    };

    Ok(config.run(
        &cli_path,
    ".",
        )
        .map_err(|e| exceptions::PyIOError::new_err(e.to_string()))?)
}

#[pymodule]
fn simplecrop_cli(_py: Python, m: &PyModule) -> PyResult<()> {
    #[pyfn(m, "run")]
    #[text_signature = "(cli_path, daily_data)"]
    fn run_py(_py: Python, cli_path: String, daily_data: &PyAny) -> PyResult<()> {
        run(_py, cli_path, daily_data)
    }

    Ok(())
}