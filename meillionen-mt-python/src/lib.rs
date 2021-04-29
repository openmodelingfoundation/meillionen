mod array;
use std::sync::Arc;

use indoc::formatdoc;
use libc::uintptr_t;
use pyo3::exceptions::{PyIOError, PyValueError, PyIndexError};
use pyo3::prelude::*;
use std;

use pythonize::{depythonize, pythonize};
use serde::{Deserialize, Serialize};

use meillionen_mt::arg::resource;
use meillionen_mt::arg::validation;
use meillionen_mt::model;
use arrow::array::{ArrayRef, make_array_from_raw, Array};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::DataType;
use std::collections::HashMap;
use std::convert::{TryInto, TryFrom};
use arrow::ffi;

fn value_error<T>(e: T) -> PyErr where T: std::fmt::Debug {
    PyValueError::new_err(format!("{:?}", e))
}

#[allow(dead_code)]
fn to_dict<T>(data: &T) -> PyResult<PyObject>
where
    T: Serialize,
{
    pythonize(Python::acquire_gil().python(), &data)
        .map_err(|e| PyValueError::new_err(format!("{:?}", e)))
}

#[allow(dead_code)]
fn from_dict<'de, T>(data: &'de PyAny) -> PyResult<T>
where
    T: Deserialize<'de>,
{
    depythonize(data).map_err(|e| PyValueError::new_err(format!("{:?}", e)))
}

fn to_py_array(array: &ArrayRef, py: Python, pa: &PyModule) -> PyResult<PyObject> {
    let (array_ptr, schema_ptr) = array.to_raw().map_err(value_error)?;
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

fn to_rust_array(obj: &PyAny) -> PyResult<ArrayRef> {
    let (array_ptr, schema_ptr) = ffi::ArrowArray::into_raw(unsafe { ffi::ArrowArray::empty() });
    obj.call_method1(
        "_export_to_c",
        (array_ptr as uintptr_t, schema_ptr as uintptr_t),
    )?;
    let array = unsafe { make_array_from_raw(array_ptr, schema_ptr).map_err(value_error)? };
    Ok(array)
}

macro_rules! impl_to_from_dict {
    ($T:ty) => {
        #[pymethods]
        impl $T {
            #[staticmethod]
            fn from_dict(data: &PyAny) -> PyResult<Self> {
                Ok(Self {
                    inner: from_dict(data)?
                })
            }

            fn to_dict(&self) -> PyResult<PyObject> {
                to_dict(&self.inner)
            }
        }
    }
}

#[pyclass]
#[derive(Debug)]
struct DataFrameValidator {
    inner: validation::DataFrameValidator
}

impl_to_from_dict!(DataFrameValidator);

#[pyclass]
#[derive(Debug)]
struct TensorValidator {
    inner: validation::TensorValidator
}

impl_to_from_dict!(TensorValidator);

#[pyclass]
#[derive(Debug)]
struct Unvalidated {
    inner: validation::Unvalidated
}

impl_to_from_dict!(Unvalidated);

#[pymethods]
impl Unvalidated {
    #[new]
    fn new() -> Self {
        Self {
            inner: validation::Unvalidated::new()
        }
    }
}

macro_rules! impl_from_arrow_array {
    ($T:ty, $Inner:ty) => {
        #[pymethods]
        impl $T {
            #[staticmethod]
            fn from_arrow_array(obj: &PyAny, index: usize) -> PyResult<Self> {
                let array = to_rust_array(obj)?;
                let b = array
                    .as_any()
                    .downcast_ref::<arrow::array::BinaryArray>()
                    .ok_or(PyValueError::new_err("arrow array is not binary"))?;
                if b.len() <= index {
                    return Err(PyIndexError::new_err(format!("index {} greater than length {}", index, b.len())))
                }
                let data = b.value(index);
                let inner: $Inner = TryFrom::try_from(data).map_err(value_error)?;
                Ok(Self { inner })
            }
        }
    }
}

macro_rules! impl_to_builder {
    ($t:ty) => {
        #[pymethods]
        impl $t {
            fn to_builder(&self, field: &str, name: &str, rb: &mut ResourceBuilder) -> PyResult<()>
            {
                let resource = std::any::type_name::<Self>();
                let inner = &self.inner;
                let data: Vec<u8> = inner
                    .try_into()
                    .map_err(value_error)?;
                rb.inner.add(field, name, resource, data.as_slice())
                    .map_err(value_error)?;
                Ok(())
            }
        }
    }
}

#[pyclass]
#[derive(Debug)]
struct ResourceBuilder {
    inner: model::ResourceBuilder
}

#[pymethods]
impl ResourceBuilder {
    #[new]
    fn __init__(program_name: &str) -> Self {
        Self {
            inner: model::ResourceBuilder::new(program_name)
        }
    }

    fn pop(&mut self) -> PyResult<PyObject> {
        let rb = self.inner.extract_to_recordbatch();
        let gil = Python::acquire_gil();
        let py = gil.python();
        let pa = py.import("pyarrow")?;
        to_recordbatch(&rb, py, pa)
    }
}

#[pyclass]
#[derive(Debug)]
struct NetCDFResource {
    inner: resource::NetCDFResource,
}

impl_to_from_dict!(NetCDFResource);
impl_from_arrow_array!(NetCDFResource, resource::NetCDFResource);
impl_to_builder!(NetCDFResource);

#[pymethods]
impl NetCDFResource {
    #[new]
    fn __init__(path: String, variable: String, dtype: &PyAny) -> PyResult<Self> {
        let data_type: DataType = depythonize(dtype)?;
        Ok(Self {
            inner: resource::NetCDFResource {
                path,
                variable,
                data_type,
                slices: HashMap::new()
            }
        })
    }
}

#[pyclass]
#[derive(Debug)]
struct FeatherResource {
    inner: resource::FeatherResource
}

impl_to_from_dict!(FeatherResource);
impl_from_arrow_array!(FeatherResource, resource::FeatherResource);
impl_to_builder!(FeatherResource);

#[pymethods]
impl FeatherResource {
    #[new]
    fn __init__(path: String) -> Self {
        Self {
            inner: resource::FeatherResource {
                path
            }
        }
    }
}

#[pyclass]
#[derive(Debug)]
struct ParquetResource {
    inner: resource::ParquetResource
}

impl_to_from_dict!(ParquetResource);
impl_from_arrow_array!(ParquetResource, resource::ParquetResource);
impl_to_builder!(ParquetResource);

#[pymethods]
impl ParquetResource {
    #[new]
    fn __init__(path: String) -> Self {
        Self {
            inner: resource::ParquetResource {
                path
            }
        }
    }
}

#[pyclass]
#[derive(Debug)]
struct FileResource {
    inner: resource::FileResource
}

impl_to_from_dict!(FileResource);
impl_from_arrow_array!(FileResource, resource::FileResource);
impl_to_builder!(FileResource);

#[pymethods]
impl FileResource {
    #[new]
    fn __init__(path: String) -> Self {
        Self {
            inner: resource::FileResource {
                path
            }
        }
    }
}

#[pyclass]
#[derive(Debug)]
struct FuncRequest {
    inner: model::FuncRequest,
}

#[pymethods]
/// A request against a function interface
impl FuncRequest {
    #[new]
    pub fn new(rb: &mut ResourceBuilder) -> PyResult<Self> {
        Ok(Self {
            inner: model::FuncRequest::try_new(rb.inner.extract_to_recordbatch())
                .map_err(value_error)?,
        })
    }

    pub fn extract_to_table(&mut self) -> PyResult<PyObject> {
        let rb = self.inner.recordbatch();
        let gil = Python::acquire_gil();
        let py = gil.python();
        let pyarrow = py.import("pyarrow")?;
        to_recordbatch(rb, py, pyarrow)
    }
}

#[pyclass]
#[derive(Debug)]
pub struct FuncInterface {
    inner: Arc<model::FuncInterface>,
}

impl FuncInterface {
    pub fn new(inner: Arc<model::FuncInterface>) -> Self {
        Self { inner }
    }
}

/// Call the model using its command line interface
///
/// :param fi: a function interface
/// :type fi: FuncInterface
/// :param program_name: the path to the program
/// :type program_name: str
/// :param fc: the request
/// :type fc: FuncRequest
/// :returns: the stdout or stderr of the program execution
/// :rtype: str
#[pyfunction]
#[text_signature = "(fi, program_name, fc, /)"]
fn client_call_cli(fi: &FuncInterface, program_name: &str, fc: &mut FuncRequest) -> PyResult<String> {
    let output = fi
        .inner
        .call_cli(program_name, &mut fc.inner)
        .map_err(|err| PyIOError::new_err(format!("{:?}", err)))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        let out = String::from_utf8_lossy(&output.stdout);
        let err = String::from_utf8_lossy(&output.stderr);
        Err(PyIOError::new_err(formatdoc! {"

            Stdout:
            {}

            Stderr:
            {}",
            out,
            err
        }))
    }
}

/// Extract arguments given to the model being executed on the command line
///
/// :param fi: a function interface
/// :type fi: FuncInterface
/// :returns: a function request
/// :rtype: FuncRequest
#[pyfunction]
#[text_signature = "(fi, /)"]
fn server_process_cli_stdin(py: Python, fi: &FuncInterface) -> PyResult<PyObject> {
    let pa = py.import("pyarrow")?;
    to_recordbatch(&fi.inner.to_cli().map_err(value_error)?.into_recordbatch(), py, pa)
}

/// Create an command line interface wrapper to a model
///
/// :param path: the local path to the model
/// :type path: str
/// :return: an interface to call the model with
/// :rtype: FuncInterface
#[pyfunction]
#[text_signature = "(path, /)"]
fn client_create_interface_from_cli(path: &str) -> PyResult<FuncInterface> {
    Ok(FuncInterface::new(Arc::new(
        model::FuncInterface::from_cli(path).map_err(|e| PyIOError::new_err(format!("{:?}", e)))?,
    )))
}

#[pymodule]
fn meillionen(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(pyo3::wrap_pyfunction!(client_call_cli, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(client_create_interface_from_cli, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(server_process_cli_stdin, m)?)?;

    m.add_class::<ResourceBuilder>()?;
    m.add_class::<FileResource>()?;
    m.add_class::<FeatherResource>()?;
    m.add_class::<NetCDFResource>()?;
    m.add_class::<ParquetResource>()?;

    m.add_class::<DataFrameValidator>()?;
    m.add_class::<TensorValidator>()?;
    m.add_class::<Unvalidated>()?;

    m.add_class::<FuncRequest>()?;
    m.add_class::<FuncInterface>()?;

    array::init(m)?;

    Ok(())
}
