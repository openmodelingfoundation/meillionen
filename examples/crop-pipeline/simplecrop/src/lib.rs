


use arrow::array::Float32Array;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use arrow::record_batch::RecordBatchReader;

use pyo3::exceptions::{PyIOError, PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes};

use model::{DailyData, SimpleCropConfig, YearlyData};
use crate::model::get_func_interface;

mod model;

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

fn run_record_batch(cli_path: String, dir: String, batch: &RecordBatch) -> eyre::Result<(RecordBatch, RecordBatch)> {
    let get_col = |name: &str| {
        get_column(&batch, name).map_err(|e| PyKeyError::new_err(e.to_string()))
    };

    let irrigation = get_col("irrigation")?;
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
}

fn run(cli_path: String, dir: String, daily_stream: StreamReader<&[u8]>) -> eyre::Result<(RecordBatch, RecordBatch)> {
    // 365 days of weather
    let mut batches: Vec<RecordBatch> = vec![];
    for b in daily_stream.into_iter() {
        match b {
            Ok(batch) => batches.push(batch),
            Err(e) => return Err(eyre::eyre!(e)),
        }
    }
    if batches.len() > 1 {
        return Err(eyre::eyre!("should only send send one record batch"));
    }

    run_record_batch(cli_path, dir, &batches[0])
}

#[pymodule]
fn simplecrop_cli(_py: Python, m: &PyModule) -> PyResult<()> {
    #[pyfn(m, "run")]
    #[text_signature = "(cli_path, dir, daily_stream_ref, /)"]
    fn run_py<'a>(_py: Python<'a>, cli_path: String, dir: String, daily_stream_ref: &[u8]) -> PyResult<(&'a PyBytes, &'a PyBytes)> {
        let daily_stream = StreamReader::try_new(daily_stream_ref)
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        let (plant, soil) = run(cli_path, dir, daily_stream)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let to_pybytes = |rb: RecordBatch| -> PyResult<&PyBytes> {
            let mut sink = Vec::<u8>::new();
            {
                let mut writer = arrow::ipc::writer::StreamWriter::try_new(
                    &mut sink, rb.schema().as_ref())
                    .map_err(|e| PyIOError::new_err(e.to_string()))?;
                writer.write(&rb);
            }
            Ok(PyBytes::new(_py, sink.as_ref()))
        };
        Ok((to_pybytes(plant)?, to_pybytes(soil)?))
    }

    #[pyfn(m, "get_func_interface")]
    fn get_func_interface_py<'a>(_py: Python<'a>) -> PyResult<&PyAny> {
        let s = serde_json::to_string(get_func_interface().as_ref())
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?;
        let json = _py.import("json")?;
        json.call("loads", (s,), None)
    }

    Ok(())
}
