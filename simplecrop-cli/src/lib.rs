mod model;
mod data;


use clap::{App, Arg, SubCommand};
use pyo3::prelude::*;
use pyo3::exceptions;
use model::{SimpleCropConfig, DailyData, YearlyData, SimpleCropDataSet};
use numpy::{IntoPyArray};

use ndarray::Array1;
use meillionen_mt::{IntoPandas, FromPandas};
use meillionen_mt_derive::{IntoPandas, FromPandas};
use crate::data::{F64CDFVariableRef, CDFStore};
use crate::model::SimpleCrop;
use std::env;
use itertools::Itertools;
use std::ffi::{OsString};

use std::collections::BTreeMap;
use pyo3::exceptions::PyIOError;
use serde_derive::{Deserialize,Serialize};
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

#[derive(IntoPandas, FromPandas)]
pub struct Coords {
    xs: Array1<f64>,
    ys: Array1<i64>
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

#[derive(Serialize, Deserialize)]
pub enum DataTypeMeta {
    Any,
    F64,
    I64
}

#[derive(Serialize, Deserialize)]
pub struct ArgumentMeta {
    datatype: DataTypeMeta,
    units: String
}

#[derive(Serialize, Deserialize)]
pub struct StoreRef(String);

impl StoreRef {
    pub fn load(&self) {
    }
}

#[derive(Serialize, Deserialize)]
pub struct FunctionCall {
    path: String,
    pub arguments: BTreeMap<String, StoreRef>
}

#[pymodule]
fn simplecrop_cli(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PySimpleCropDataSet>()?;
    m.add_class::<F64CDFVariableRef>()?;
    m.add_class::<CDFStore>()?;
    m.add_class::<SimpleCrop>()?;

    #[pyfn(m, "simplecrop_run")]
    fn simplecrop_run_py(_py: Python, s: String) -> PyResult<()> {
        let fc: FunctionCall = serde_json::from_str(s.as_str())
            .or(Err(PyErr::from(PyIOError::new_err("could not deserialize call"))))?;
        let _daily = fc.arguments
            .get("daily")
            .or(Some(&StoreRef("daily.parquet".to_string())))
            .ok_or(PyErr::from(PyIOError::new_err("could not open create store ref")))?;
        Ok(())
    }

    #[pyfn(m, "run_cli")]
    fn run_cli_py(_py: Python) -> PyResult<()> {
        let matches = App::new("simplecrop")
            .version("0.1")
            .about("The simplecrop model")
            .author("Open Modeling Foundation")
            .subcommand(SubCommand::with_name("constructor")
                .about("arguments to pass to the simplecrop model"))
            .subcommand(SubCommand::with_name("run")
                .arg(Arg::with_name("cli_path")
                    .help("path to simplecrop cli")
                    .required(true))
                .arg(Arg::with_name("daily")
                    .help("daily data dataframe")
                    .required(true)))
            .get_matches_from(vec![OsString::from("simplecrop")].into_iter().chain(env::args_os().dropping(2)));
        if let Some(_constructor) = matches.subcommand_matches("constructor") {
            println!("constructor")
        } else if let Some(run) = matches.subcommand_matches("run") {
            let cli_path = run.value_of("cli_path").unwrap();
            let daily = run.value_of("daily").unwrap();
            let pd = PyModule::import(_py, "pandas")?;
            let daily_data = pd.call("read_parquet", (), Some([("path", daily)].into_py_dict(_py)))?;
            run(_py, cli_path.to_string(), daily_data)
        }
        Ok(())
    }

    #[pyfn(m, "run")]
    #[text_signature = "(cli_path, daily_data)"]
    fn run_py(_py: Python, cli_path: String, daily_data: &PyAny) -> PyResult<()> {
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
        let coords = Coords {
          xs: Array1::from(vec![1.5,4.0,5.0,6.0]),
          ys: Array1::from(vec![3,5,6,2])
        };
        coords.into_pandas(_py)
    }

    Ok(())
}