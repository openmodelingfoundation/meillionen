extern crate simplecrop_cli;

use chrono::{DateTime, NaiveDateTime, Utc};
use pyo3::prelude::*;
use pyo3::exceptions;
use simplecrop_cli::{PlantConfig, SoilConfig, SimCtnlConfig, SimpleCropConfig, WeatherDataset, IrrigationDataset, execute, PlantDataSet, SoilDataSet, Weather, Irrigation};

#[pyclass]
#[derive(Debug)]
pub struct PySoilDataSet { inner: SoilDataSet }

#[pymethods]
impl PySoilDataSet {
    fn print(&self, _py: Python) {
        print!("{:?}", self.inner)
    }
}

#[pyclass]
#[derive(Debug)]
pub struct PyPlantDataSet { inner: PlantDataSet }

#[pymethods]
impl PyPlantDataSet {
    fn print(&self) {
        print!("{:?}", self.inner)
    }
}

fn run(cli_path: String, current_dir_path: String) -> PyResult<(PyPlantDataSet, PySoilDataSet)> {
    // 365 days of weather
    let weather_dataset = WeatherDataset((1..1001).map(|doy| Weather {
        date: DateTime::from_utc(NaiveDateTime::from_timestamp(87000 + doy, 0), Utc),
        srad: 5.1,
        tmax: 20.0,
        tmin: 4.4,
        rain: 23.9,
        par: 10.7,
    }).collect());

    // > 293 days of irrigation
    let irrigation_dataset = IrrigationDataset((1..1001).map(|doy| Irrigation {
        date: DateTime::from_utc(NaiveDateTime::from_timestamp(87000 + doy, 0), Utc),
        amount: 0f32
    }).collect());

    let plant = PlantConfig {
        lfmax: 12.0,
        emp2: 0.64,
        emp1: 0.104,
        pd: 5.0,
        nb: 5.3,
        rm: 0.100,
        fc: 0.85,
        tb: 10.0,
        intot: 300.0,
        n: 2.0,
        lai: 0.013,
        w: 0.3,
        wr: 0.045,
        wc: 0.255,
        p1: 0.03,
        f1: 0.028,
        sla: 0.035,
    };

    let soil = SoilConfig {
        wpp: 0.06,
        fcp: 0.17,
        stp: 0.28,
        dp: 145.00,
        drnp: 0.10,
        cn: 55.00,
        swc: 246.50,
    };

    let simcntl = SimCtnlConfig {
        doyp: 121,
        frop: 3,
    };

    let config = SimpleCropConfig {
        irrigation_dataset,
        plant,
        soil,
        simctrl: simcntl,
        weather_dataset,
    };

    let r = execute(
        &config,
        &current_dir_path,
        &cli_path)
        .map_err(|e| exceptions::IOError::py_err(e.to_string()))?;
    Ok((PyPlantDataSet { inner: r.0.plant }, PySoilDataSet { inner: r.0.soil }))
}

#[pymodule]
fn simplecrop_cli_python(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PySoilDataSet>()?;
    m.add_class::<PyPlantDataSet>()?;

    #[pyfn(m, "run")]
    #[text_signature = "(cli_path, current_dir_path, /)"]
    fn run_py(_py: Python, cli_path: String, current_dir_path: String) -> PyResult<(PyPlantDataSet, PySoilDataSet)> {
        run(cli_path, current_dir_path)
    }

    Ok(())
}