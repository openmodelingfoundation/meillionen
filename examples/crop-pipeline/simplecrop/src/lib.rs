use arrow::array::{Float32Array, ArrayRef};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use libc::uintptr_t;
use pyo3::exceptions::{PyIOError, PyKeyError, PyValueError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use model::{DailyData, SimpleCropConfig, YearlyData};

use crate::model::get_func_interface;
use stable_eyre::eyre::WrapErr;

mod model;

fn get_column<'a>(batch: &'a RecordBatch, name: &str) -> stable_eyre::Result<&'a [f32]> {
    let schema = batch.schema();
    let col_ind = schema.index_of(name).map_err(|e| stable_eyre::eyre::eyre!(e))?;
    let col = batch.column(col_ind);
    use arrow::datatypes::DataType::Float32;
    col.as_any()
        .downcast_ref::<Float32Array>()
        .ok_or(stable_eyre::eyre::eyre!(
            "column {} type mismatch: expected {:?} got {:?}",
            name,
            col.data_type(),
            Float32
        ))
        .map(|a| a.values())
}

fn run_record_batch(
    cli_path: String,
    dir: String,
    daily_batch: &RecordBatch,
    yearly_batch: &RecordBatch,
) -> stable_eyre::Result<(RecordBatch, RecordBatch)> {
    let get_col =
        |name: &str| get_column(&daily_batch, name).map_err(|e| PyKeyError::new_err(e.to_string()));

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

    let yearly = YearlyData::from_recordbatch_row(yearly_batch, 0).unwrap();

    let config = SimpleCropConfig { daily, yearly };

    config.run(&cli_path, &dir)
}

fn to_py_array(array: &ArrayRef, py: Python, pa: &PyModule) -> PyResult<PyObject> {
    let (array_ptr, schema_ptr) = array.to_raw()
        .map_err(|e| PyValueError::new_err(format!("{:?}", e)))?;
    let array = pa.getattr("Array")?
        .call_method1(
            "_import_from_c",
            (array_ptr as uintptr_t, schema_ptr as uintptr_t))?;
    Ok(array.to_object(py))
}

fn to_recordbatch(rb: &RecordBatch, py: Python, pa: &PyModule) -> PyResult<PyObject> {
    let schema = rb.schema();
    let arrays = rb.columns().iter().map(|a| to_py_array(a, py, pa)).collect::<PyResult<Vec<PyObject>>>()?;
    let names = schema.fields().iter().map(|f| f.name().as_str()).collect::<Vec<&str>>();
    let record = pa
        .getattr("RecordBatch")?
        .call_method1("from_arrays", (arrays, names))?;
    Ok(record.to_object(py))
}

fn run(
    cli_path: String,
    dir: String,
    daily_stream: StreamReader<&[u8]>,
    yearly_stream: StreamReader<&[u8]>,
) -> stable_eyre::Result<(RecordBatch, RecordBatch)> {
    // 365 days of weather
    let stream_convert = |stream: StreamReader<&[u8]>| -> stable_eyre::Result<RecordBatch> {
        let rc = stream
            .into_iter()
            .next()
            .ok_or(stable_eyre::eyre::eyre!("Stream was empty")).unwrap()
            .map_err(|e| stable_eyre::eyre::eyre!(e)).unwrap();
        Ok(rc)
    };
    let daily_batch = stream_convert(daily_stream).unwrap();
    let yearly_batch = stream_convert(yearly_stream).unwrap();
    run_record_batch(cli_path, dir, &daily_batch, &yearly_batch)
}

#[pymodule]
fn simplecrop_omf(_py: Python, m: &PyModule) -> PyResult<()> {
    #[pyfn(m, "run")]
    #[text_signature = "(cli_path, dir, daily_stream_ref, year_stream_ref, /)"]
    fn run_py<'a>(
        _py: Python<'a>,
        cli_path: String,
        dir: String,
        daily_stream_ref: &[u8],
        yearly_stream_ref: &[u8],
    ) -> PyResult<(&'a PyBytes, &'a PyBytes)> {
        let daily_stream = StreamReader::try_new(daily_stream_ref)
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        let yearly_stream = StreamReader::try_new(yearly_stream_ref)
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        let (plant, soil) = run(cli_path, dir, daily_stream, yearly_stream)
            .map_err(|e| PyRuntimeError::new_err(format!("{:?}", e)))?;
        let to_pybytes = |rb: RecordBatch| -> PyResult<&PyBytes> {
            let mut sink = Vec::<u8>::new();
            {
                let mut writer =
                    arrow::ipc::writer::StreamWriter::try_new(&mut sink, rb.schema().as_ref())
                        .map_err(|e| PyIOError::new_err(e.to_string()))?;
                writer.write(&rb).wrap_err("Cannot write record batch")
                    .map_err(|e| PyValueError::new_err(format!("{:?}", e)))?;
            }
            Ok(PyBytes::new(_py, sink.as_ref()))
        };
        Ok((to_pybytes(plant)?, to_pybytes(soil)?))
    }

    #[pyfn(m, "get_func_interface")]
    fn get_func_interface_py(_py: Python) -> PyResult<PyObject> {
        let rb = get_func_interface()
            .map_err(|e| PyValueError::new_err(format!("{:?}", e)))?
            .into_recordbatch();
        let pyarrow = _py.import("pyarrow")?;
        to_recordbatch(&rb, _py, pyarrow)
    }

    Ok(())
}
