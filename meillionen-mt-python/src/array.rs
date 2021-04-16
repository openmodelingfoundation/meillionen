use arrow::array::{ArrayRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::{SerializedFileReader};
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;
use pyo3::types::{PyModule};
use pyo3::{PyErr, PyObject, PyResult, Python, ToPyObject};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

fn to_pyarrow_array(array: &ArrayRef, py: Python) -> PyResult<PyObject> {
    let (array_ptr, schema_ptr) = array
        .to_raw()
        .map_err(|e| PyValueError::new_err(format!("{}", e)))?;

    let pa = py.import("pyarrow")?;
    let array = pa.getattr("Array")?.call_method1(
        "_import_from_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    Ok(array.to_object(py))
}

fn to_pyarrow_recordbatch<'a>(
    batch: &RecordBatch,
    py: Python,
    pyarrow: &'a PyModule,
) -> Result<PyObject, PyErr> {
    let mut py_arrays = vec![];
    let mut py_names = vec![];

    let schema = batch.schema();
    for (array, field) in batch.columns().iter().zip(schema.fields().iter()) {
        let array = to_pyarrow_array(array, py)?;

        py_arrays.push(array);
        py_names.push(field.name())
    }

    let record = pyarrow
        .getattr("RecordBatch")?
        .call_method1("from_arrays", (py_arrays, py_names))?;

    Ok(record.to_object(py))
}

fn to_pyarrow_table(batches: &Vec<RecordBatch>) -> PyResult<PyObject> {
    let gil = pyo3::Python::acquire_gil();
    let py = gil.python();
    let pyarrow = PyModule::import(py, "pyarrow")?;

    let mut py_batches = vec![];
    for batch in batches {
        py_batches.push(to_pyarrow_recordbatch(batch, py, pyarrow)?);
    }
    let result = pyarrow
        .getattr("Table")?
        .call_method1("from_batches", (py_batches,))?;
    Ok(result.to_object(py))
}

fn ioerror<T>(e: T) -> PyErr
where
    T: std::error::Error,
{
    PyIOError::new_err(format!("{}", e))
}

#[pyfunction]
fn from_parquet(path: &str) -> PyResult<PyObject> {
    let file = File::open(&Path::new(path)).map_err(ioerror)?;
    let file_reader = SerializedFileReader::new(file).map_err(ioerror)?;
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));
    let mut record_batch_reader = arrow_reader.get_record_reader(2048).map_err(ioerror)?;
    let mut batches = vec![];
    while let Some(Ok(record_batch)) = record_batch_reader.next() {
        batches.push(record_batch);
    }
    let table = to_pyarrow_table(&batches);
    table
}

pub fn init(m: &PyModule) -> PyResult<()> {
    m.add_function(pyo3::wrap_pyfunction!(from_parquet, m)?)?;
    Ok(())
}
