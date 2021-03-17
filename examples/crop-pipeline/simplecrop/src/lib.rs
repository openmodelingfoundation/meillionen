mod model;

use model::{DailyData, SimpleCropConfig, SimpleCropDataSet, YearlyData};
use pyo3::exceptions;
use pyo3::prelude::*;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use arrow::record_batch::RecordBatchReader;
use pyo3::exceptions::PyIOError;

#[pyclass]
#[derive(Debug)]
pub struct PySimpleCropDataSet {
    inner: SimpleCropDataSet,
}

#[pymethods]
impl PySimpleCropDataSet {
    fn print(&self, _py: Python) {
        print!("{:?}", self.inner)
    }
}

fn run(cli_path: String, dir: String, daily_stream: StreamReader<&[u8]>) -> PyResult<()> {
    let schema = Schema::new(vec![
        Field::new("irrigation", DataType::Float32, false),
        Field::new("temp_max", DataType::Float32, false),
        Field::new("temp_min", DataType::Float32, false),
        Field::new("rainfall", DataType::Float32, false),
        Field::new("photosynthetic_energy_flux", DataType::Float32, false),
        Field::new("energy_flux", DataType::Float32, false),
    ]);

    // 365 days of weather
    let mut batches: Vec<RecordBatch> = vec![];
    for b in daily_stream.into_iter() {
        match b {
            Ok(batch) => batches.push(batch),
            Err(e) => return Err(PyIOError::new_err(e.to_string())),
        }
    }

    if &schema == batches[0].schema().as_ref() {
        return Err(PyIOError::new_err("schema mismatch"));
    }

    let extract_col = |col_ind: usize| {
        batches[0]
            .column(col_ind)
            .as_any()
            .downcast_ref::<arrow::array::Float32Array>()
    };

    let _irrigation = extract_col(0).unwrap();
    let irrigation = _irrigation.values();

    let _temp_max = extract_col(1).unwrap();
    let temp_max = _temp_max.values();

    let _temp_min = extract_col(2).unwrap();
    let temp_min = _temp_min.values();

    let _rainfall = extract_col(3).unwrap();
    let rainfall = _rainfall.values();

    let _photosynthetic_energy_flux = extract_col(4).unwrap();
    let photosynthetic_energy_flux = _photosynthetic_energy_flux.values();

    let _energy_flux = extract_col(5).unwrap();
    let energy_flux = _energy_flux.values();

    let daily = DailyData {
        irrigation,
        temp_max,
        temp_min,
        rainfall,
        photosynthetic_energy_flux,
        energy_flux,
    };

    let yearly = YearlyData::default();

    let config = SimpleCropConfig { daily, yearly };

    config
        .run(&cli_path, &dir)
        .map_err(|e| exceptions::PyIOError::new_err(e.to_string()))
}

#[pymodule]
fn simplecrop_cli(_py: Python, m: &PyModule) -> PyResult<()> {
    #[pyfn(m, "run")]
    #[text_signature = "(cli_path, daily_stream_ref, /)"]
    fn run_py(_py: Python, cli_path: String, dir: String, daily_stream_ref: &[u8]) -> PyResult<()> {
        let daily_stream = StreamReader::try_new(daily_stream_ref)
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        run(cli_path, dir, daily_stream)
    }

    Ok(())
}
