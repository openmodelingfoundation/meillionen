extern crate simplecrop_cli;
extern crate arrow;
extern crate lazy_static;

use chrono::{DateTime, NaiveDateTime, Utc};
use pyo3::prelude::*;
use pyo3::exceptions;
use simplecrop_cli::{PlantConfig, SoilConfig, SimCtnlConfig, SimpleCropConfig, WeatherDataset, IrrigationDataset, PlantDataSet, SoilDataSet, Weather, Irrigation, execute_in_tempdir};
use numpy::{PyArray1, ToPyArray};
use arrow::array::{Float32Array};

mod dataframes;

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

fn run(cli_path: String) -> PyResult<(PyPlantDataSet, PySoilDataSet)> {
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

    let r = execute_in_tempdir(
        &cli_path,
        &config,
        )
        .map_err(|e| exceptions::IOError::py_err(e.to_string()))?;
    Ok((PyPlantDataSet { inner: r.0.plant }, PySoilDataSet { inner: r.0.soil }))
}

#[pymodule]
fn simplecrop_cli_python(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PySoilDataSet>()?;
    m.add_class::<PyPlantDataSet>()?;

    #[pyfn(m, "run")]
    #[text_signature = "(cli_path, /)"]
    fn run_py(_py: Python, cli_path: String) -> PyResult<(PyPlantDataSet, PySoilDataSet)> {
        run(cli_path)
    }

    #[pyfn(m, "run_crop_model")]
    #[text_signature = "(*, plant_config)"]
    fn run_crop_model_py<'py>(_py: Python<'py>, plant_config: &PyAny) -> PyResult<&'py PyArray1<f32>> {
        let mut cols = Vec::new();
        for attr in ["wpp", "fcp"].iter() {
            let col: &PyArray1<f32> = plant_config.get_item(attr)?.extract()?;
            let ro = col.readonly();
            let s = ro.as_slice()?;
            let mut builder = Float32Array::builder(s.len());
            builder.append_slice(s).unwrap();
            let arr = builder.finish();
            cols.push(arr);
        }
        let added = arrow::compute::add(&cols[0], &cols[1]).unwrap();
        let r = added.value_slice(0, added.len());
        Ok(r.to_pyarray(_py))
    }

    Ok(())
}