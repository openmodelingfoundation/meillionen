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
use arrow::datatypes::{Field, Schema};
use std::convert::{TryInto, TryFrom};
use arrow::ffi;
use pyo3::types::PyDict;

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

fn to_py_recordbatch(rb: &RecordBatch, py: Python, pa: &PyModule) -> PyResult<PyObject> {
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

fn to_rust_recordbatch(obj: &PyAny) -> PyResult<RecordBatch> {
    let pycolumns = obj.getattr("columns")?;
    let names: Vec<String> = obj.getattr("schema")?.getattr("names")?.extract()?;
    let mut columns = vec![];
    let mut fields = vec![];
    for (pycol, name) in pycolumns.iter()?.zip(names) {
        let pycol = pycol?;
        let col = to_rust_array(pycol)?;
        let dt = col.data_type().clone();
        let nullable = col.null_count() > 0;
        columns.push(col);
        fields.push(Field::new(name.as_str(), dt, nullable))
    }
    let rb = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    Ok(rb)
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

macro_rules! impl_transformers {
    ($T:ty) => {
        #[pymethods]
        impl $T {
            #[staticmethod]
            fn transformers(_py: Python) -> &PyDict {
                PyDict::new(_py)
            }
        }
    };
    ($T:ty, $(($prop:ident, $transformer:ident)),*) => {
        #[pymethods]
        impl $T {
            #[staticmethod]
            fn transformers(_py: Python) -> PyResult<&PyDict> {
                let dict = PyDict::new(_py);
                $(dict.set_item(std::stringify!($prop), std::stringify!($transformer))
                    .map_err(value_error)?;);*
                Ok(dict)
            }
        }
    }
}

macro_rules! impl_name_prop {
    ($T:ty, $name:literal) => {
        #[pymethods]
        impl $T {
            #[classattr]
            fn name() -> &'static str {
                std::any::type_name::<Self>()
            }

            #[classattr]
            fn function_name() -> &'static str {
                $name
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
impl_from_arrow_array!(DataFrameValidator, validation::DataFrameValidator);
impl_to_builder!(DataFrameValidator);
impl_name_prop!(DataFrameValidator, "data_frame");
impl_transformers!(DataFrameValidator);

#[pyclass]
#[derive(Debug)]
struct TensorValidator {
    inner: validation::TensorValidator
}

impl_to_from_dict!(TensorValidator);
impl_from_arrow_array!(TensorValidator, validation::TensorValidator);
impl_to_builder!(TensorValidator);
impl_name_prop!(TensorValidator, "tensor");
impl_transformers!(TensorValidator);

#[pyclass]
#[derive(Debug)]
struct Unvalidated {
    inner: validation::Unvalidated
}

impl_to_from_dict!(Unvalidated);
impl_from_arrow_array!(Unvalidated, validation::Unvalidated);
impl_to_builder!(Unvalidated);
impl_name_prop!(Unvalidated, "unvalidated");
impl_transformers!(Unvalidated);

#[pymethods]
impl Unvalidated {
    #[new]
    fn new(ext: &str) -> Self {
        Self {
            inner: validation::Unvalidated::new(ext)
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
        to_py_recordbatch(&rb, py, pa)
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
impl_name_prop!(NetCDFResource, "netcdf");
impl_transformers!(NetCDFResource, (path, path_transformer));

#[pymethods]
impl NetCDFResource {
    #[new]
    fn __init__(path: String, variable: String) -> PyResult<Self> {
        Ok(Self {
            inner: resource::NetCDFResource {
                path,
                variable,
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
impl_name_prop!(FeatherResource, "feather");
impl_transformers!(FeatherResource, (path, path_transformer));

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
impl_name_prop!(ParquetResource, "parquet");
impl_transformers!(ParquetResource, (path, path_transformer));

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
impl_name_prop!(FileResource, "file");
impl_transformers!(FileResource, (path, path_transformer));

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
#[text_signature = "(program_name, fc, /)"]
fn client_call_cli(program_name: &str, pyrb: &PyAny) -> PyResult<String> {
    let request = to_rust_recordbatch(pyrb)?;
    let output = model::client_call_cli(program_name, &request)
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
/// :param pyrb: an interface record batch
/// :type pyrb: RecordBatch
/// :returns: a request record batch
/// :rtype: FuncRequest
#[pyfunction]
#[text_signature = "(pyrb, /)"]
fn server_respond_from_cli(py: Python, name: &str, pyrb: &PyAny) -> PyResult<PyObject> {
    let interface = to_rust_recordbatch(pyrb)?;
    let request = model::server_respond_from_cli(name, &interface)
        .map_err(value_error)?;
    let pa = py.import("pyarrow")?;
    to_py_recordbatch(&request, py, pa)
}

/// Create an command line interface wrapper to a model
///
/// :param path: the local path to the model
/// :type path: str
/// :return: an interface to call the model with
/// :rtype: FuncInterface
#[pyfunction]
#[text_signature = "(path, /)"]
fn client_create_interface_from_cli(path: &str) -> PyResult<PyObject> {
    let rb = model::client_create_interface_from_cli(path)
        .map_err(|e| PyIOError::new_err(format!("{:?}", e)))?;
    let gil = Python::acquire_gil();
    let py = gil.python();
    let pa = py.import("pyarrow")?;
    to_py_recordbatch(&rb, py, pa)
}

#[pymodule]
fn meillionen(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(pyo3::wrap_pyfunction!(client_call_cli, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(client_create_interface_from_cli, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(server_respond_from_cli, m)?)?;

    m.add_class::<ResourceBuilder>()?;
    m.add_class::<FileResource>()?;
    m.add_class::<FeatherResource>()?;
    m.add_class::<NetCDFResource>()?;
    m.add_class::<ParquetResource>()?;

    m.add_class::<DataFrameValidator>()?;
    m.add_class::<TensorValidator>()?;
    m.add_class::<Unvalidated>()?;

    array::init(m)?;

    Ok(())
}
