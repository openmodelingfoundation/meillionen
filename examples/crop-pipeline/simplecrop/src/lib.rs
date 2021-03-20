mod model;

use model::{DailyData, SimpleCropConfig, YearlyData};
use pyo3::exceptions;
use pyo3::prelude::*;

use arrow::datatypes::{DataType, Field, Schema, Float32Type};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use arrow::record_batch::RecordBatchReader;
use pyo3::exceptions::{PyIOError, PyKeyError};
use arrow::array::Float32Array;

fn get_column<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a [f32]> {
    let schema = batch.schema();
    let col_ind = schema.index_of(name)
        .map_err(|e| eyre::eyre!(e))?;
    let col = batch.column(col_ind);
    use arrow::datatypes::DataType::Float32;
    col
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or(eyre::eyre!("column {} type mismatch: expected {:?} got {:?}", name, col.data_type(), Float32))
        .map(|a| a.values())
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

    let get_col = |name: &str| {
        get_column(&batches[0], name).map_err(|e| PyKeyError::new_err(e.to_string()))
    };

    let irrigation =  get_col("irrigation")?;
    let temp_max = get_col("temp_max")?;
    let temp_min = get_col("temp_min")?;
    let rainfall = get_col("rainfall")?;
    let photosynthetic_energy_flux = get_col("photosynthetic_energy_flux")?;
    let energy_flux = get_col("energy_flux")?;

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
    #[text_signature = "(cli_path, dir, daily_stream_ref, /)"]
    fn run_py(_py: Python, cli_path: String, dir: String, daily_stream_ref: &[u8]) -> PyResult<()> {
        let daily_stream = StreamReader::try_new(daily_stream_ref)
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        run(cli_path, dir, daily_stream)
    }

    Ok(())
}
