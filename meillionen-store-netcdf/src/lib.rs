use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use netcdf::Variable;
use arrow::datatypes;
use arrow::array::{ArrayRef, Int64Array, Int64Builder, Float64Builder, Float64Array};
use thiserror::Error;
use std::convert::TryFrom;
use std::collections::BTreeMap;
use std::fmt::Display;
use itertools::Itertools;
use std::borrow::BorrowMut;
use arrow::datatypes::Field;

#[derive(Error, Debug)]
pub enum SchemaValidation {
    #[error("type mismatch in column {name:?}, expected {expected:?} received {received:?}")]
    TypeMismatch {
        name: String,
        expected: VarType,
        received: VarType
    }
}

#[derive(Error, Debug)]
pub enum StoreRetrieveError {
    #[error("dimension mismatch, expected {expected:?} but received {received:?}")]
    DimMismatch {
        expected: usize,
        received: usize
    },
    #[error("variable {0} not found")]
    NotFound(String)
}

#[derive(Error, Debug)]
pub enum SourcePutError {
    #[error("type mismatch for variable {name}, expected {expected:?} but received {received:?}")]
    TypeMismatch {
        name: String,
        expected: VarType,
        received: VarType
    },
    #[error("library from base error library {0}")]
    Base(String),
    #[error("variable {0} not found")]
    NotFound(String)
}

#[derive(Clone, Debug)]
pub enum VarType {
    F64,
    I64,
    String
}

impl VarType {
    fn to_arrow(&self) -> datatypes::DataType {
        match self {
            VarType::F64 => datatypes::DataType::Float64,
            VarType::I64 => datatypes::DataType::Int64,
            VarType::String => datatypes::DataType::Utf8
        }
    }

    fn from_arrow(dt: &datatypes::DataType) -> Option<Self> {
        match dt {
            datatypes::DataType::Float64 => Some(VarType::F64),
            datatypes::DataType::Int64 => Some(VarType::I64),
            datatypes::DataType::Utf8 => Some(VarType::String),
            _ => None
        }
    }
}

#[derive(Clone)]
pub struct Var {
    name: String,
    tp: VarType
}

impl Var {
    pub fn new(name: String, tp: VarType) -> Self {
        Self {
            name,
            tp
        }
    }
}

pub struct Schema {
    vars: Vec<Var>
}

impl Schema {
    pub fn new(vars: Vec<Var>) -> Self {
        Self {
            vars
        }
    }

    fn get_type(&self, name: &str) -> Option<&VarType> {
        Some(&self.vars.iter().find(|v| v.name == name)?.tp)
    }



    pub fn to_schema(&self) -> datatypes::SchemaRef {
        Arc::new(
            datatypes::Schema::new(
                self.vars.iter()
                    .map(|v| datatypes::Field::new(v.name.as_ref(), v.tp.to_arrow(), false))
                    .collect()))
    }

    pub(crate) fn add_vars(&mut self, schema: &Schema) {
        for v in schema.vars.iter() {
            self.vars.push(v.clone());
        }
    }
}

trait Source {
    fn schema(&self) -> &Schema;
    fn put_into(&mut self, q: &Query, rb: &RecordBatch) -> Result<(), SourcePutError>;
    fn retrieve(&self, q: &Query) -> Result<RecordBatch, StoreRetrieveError>;
    fn variables(&self) -> Vec<String>;
}

pub struct Filter {
    name: String,
    i: usize
}

impl Filter {
    pub fn new(name: String, i: usize) -> Self {
        Self {
            name,
            i
        }
    }
}

pub struct Query {
    select: String,
    filters: Vec<Filter>
}

impl Query {
    pub fn new(select: String, filters: Vec<Filter>) -> Self {
        Self {
            select,
            filters
        }
    }

    pub fn get_filter(&self, name: &str) -> Option<&Filter> {
        self.filters.iter().find(|f| f.name == name)
    }
}

#[derive(Debug)]
pub struct MemorySource {
    pub schema: Schema,
    pub rb: RecordBatch
}

impl MemorySource {
    pub fn try_new(rb: RecordBatch) -> Option<Self> {
        let schema = Schema::new(rb.schema()
            .fields()
            .iter()
            .map(|f| {
                let tp = VarType::from_arrow(f.data_type());
                tp.map(|vt| Var::new(f.name().clone(), vt))
            })
            .collect::<Option<Vec<Var>>>()?);
        Some(Self {
            schema,
            rb
        })
    }
}

impl Source for MemorySource {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn put_into(&mut self, q: &Query, rb: &RecordBatch) -> Result<(), SourcePutError> {
        let rbm = self.rb.borrow_mut();
    }

    fn retrieve(&self, q: &Query) -> Result<RecordBatch, StoreRetrieveError> {
        use StoreRetrieveError::NotFound;
        let (pos, f) = self.rb.schema().fields().iter()
            .find_position(|f| f.name().as_ref() == q.select.as_ref())
            .ok_or_else(|| NotFound(q.select.clone()))?;
        let variable = self.rb.column(pos);
        Ok(RecordBatch::try_new(
            Arc::new(datatypes::Schema::new(vec![
                Field::new(f.name().as_ref(), f.data_type().clone(), false)
            ])),
            vec![variable.clone()]
        ).unwrap())
    }

    fn variables(&self) -> Vec<String> {
        self.schema.vars.iter().map(|v| v.name.clone()).collect()
    }
}

#[derive(Debug)]
pub struct CDFSliceData {
    pub capacity: usize,
    pub indices: Vec<usize>,
    pub slice_len: Vec<usize>,
    pub strides: Vec<isize>
}

pub struct CDFSource {
    pub schema: Schema,
    pub file: netcdf::MutableFile
}

impl CDFSource {
    pub fn try_new(path: String, schema: Schema) -> netcdf::error::Result<Self> {
        let file = netcdf::append(path)?;
        Ok(Self {
            schema,
            file
        })
    }

    fn retrieve_f64(variable: &netcdf::Variable, sd: &CDFSliceData) -> ArrayRef {
        let mut b = vec![0f64; sd.capacity];
        let n = sd.slice_len.len();

        variable.values_strided_to(
            b.as_mut_slice(),
            Some(sd.indices.as_slice()),
            Some(sd.slice_len.as_slice()),
            sd.strides.as_slice()).unwrap();

        let mut res = Float64Builder::new(n);
        res.append_slice(b.as_slice());
        Arc::new(res.finish())
    }

    fn retrieve_i64(variable: &netcdf::Variable, sd: &CDFSliceData) -> ArrayRef {
        let mut b = vec![0i64; sd.capacity];
        let n = sd.slice_len.len();

        variable.values_strided_to(
            b.as_mut_slice(),
            Some(sd.indices.as_slice()),
            Some(sd.slice_len.as_slice()),
            sd.strides.as_slice()).unwrap();

        let mut res= Int64Builder::new(n);
        res.append_slice(b.as_slice());
        Arc::new(res.finish())
    }

    fn retrieve_str(variable: &netcdf::Variable, sd: &CDFSliceData) -> ArrayRef {
        let mut b = arrow::array::StringBuilder::new(sd.capacity);
        Arc::new(b.finish())
    }

    fn put_f64(variable: &mut netcdf::VariableMut, name: &str, data: &ArrayRef, sd: &CDFSliceData) -> Result<(), SourcePutError> {
        use SourcePutError::*;
        let arr = data.as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| TypeMismatch {
                name: name.to_string(),
                expected: VarType::F64,
                received: VarType::from_arrow(data.data_type()).unwrap()
            })?;
        let slice = arr.value_slice(0, arr.len());
        variable
            .put_values_strided(slice, Some(&sd.indices), Some(&sd.slice_len), &sd.strides)
            .map_err(|e| Base(e.to_string()))?;
        Ok(())
    }

    fn put_i64(variable: &mut netcdf::VariableMut, name: &str, data: &ArrayRef, sd: &CDFSliceData) -> Result<(), SourcePutError> {
        use SourcePutError::*;
        let arr = data.as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| TypeMismatch {
                name: name.to_string(),
                expected: VarType::I64,
                received: VarType::from_arrow(data.data_type()).unwrap()
            })?;
        let slice = arr.value_slice(0, arr.len());
        variable
            .put_values_strided(slice, Some(&sd.indices), Some(&sd.slice_len), &sd.strides)
            .map_err(|e| Base(e.to_string()))?;
        Ok(())
    }

    fn put_str(variable: &mut netcdf::VariableMut, name: &str, data: &ArrayRef, sd: &CDFSliceData) -> Result<(), SourcePutError> {
        Ok(())
    }

    fn build_slice(variable: &netcdf::Variable, q: &Query) -> CDFSliceData {
        let n = variable.dimensions().len();
        let slices = variable.dimensions().iter()
            .map(|d|
                q.get_filter(d.name().as_str())
                    .map(|f| 1)
                    .unwrap_or(d.len())
            );

        let slice_len = slices.collect::<Vec<usize>>();
        let capacity: usize = slice_len.iter().product();

        let indices = variable.dimensions().iter()
            .map(|d| {
                q.get_filter(d.name().as_str()).map(|f| f.i).unwrap_or(0)
            })
            .collect::<Vec<usize>>();

        let strides: Vec<isize> = vec![1; n];

        CDFSliceData {
            capacity,
            indices,
            slice_len,
            strides
        }
    }

    fn validate(&self, rb: &RecordBatch) -> Result<(), Vec<SchemaValidation>> {
        use SchemaValidation::TypeMismatch;
        let mut errors = Vec::new();
        for f in rb.schema().fields() {
            if let Some(s) = self.schema.get_type(f.name()) {
                if s.to_arrow() != f.data_type().clone() {
                    VarType::from_arrow(f.data_type())
                        .map(|vt| TypeMismatch {
                            name: f.name().clone(),
                            expected: s.clone(),
                            received: VarType::F64
                        });
                }
            }
        }
        if errors.len() > 0 {
            return Err(errors)
        }
        Ok(())
    }
}

impl Source for CDFSource {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn put_into(&mut self, q: &Query, rb: &RecordBatch) -> Result<(), SourcePutError> {
        use SourcePutError::{NotFound};
        let fname= &q.select;
        let (pos, field) = rb.schema().fields().iter()
            .find_position(|f| f.name() == fname)
            .ok_or_else(|| NotFound(fname.to_string()))?;
        let data = rb.column(pos);

        let mut variable = self.file.variable_mut(fname).ok_or_else(|| NotFound(fname.to_string()))?;
        let sd = CDFSource::build_slice(&variable, q);

        if let Some(vt) = VarType::from_arrow(data.data_type()) {
             let r = match vt {
                 VarType::F64 => CDFSource::put_f64(&mut variable, fname, data, &sd),
                 VarType::I64 => CDFSource::put_i64(&mut variable, fname, data, &sd),
                 VarType::String => CDFSource::put_str(&mut variable, fname, data, &sd)
             };
             return r
        }
        Ok(())
    }

    fn retrieve(&self, q: &Query) -> Result<RecordBatch, StoreRetrieveError> {
        let variable = self.file
            .variable(q.select.as_ref())
            .ok_or_else(|| StoreRetrieveError::NotFound(q.select.to_string()))?;
        let n = variable.dimensions().len();
        if n - 1  != q.filters.len() {
            return Err(StoreRetrieveError::DimMismatch { expected: n - 1, received: q.filters.len()})
        }
        let tp = self.schema.get_type(q.select.as_ref()).ok_or_else(|| StoreRetrieveError::NotFound(q.select.clone()))?;

        let sd = CDFSource::build_slice(&variable, q);

        println!("indices: {:?}", sd.indices);
        println!("slice_len: {:?}", sd.slice_len);
        println!("strides: {:?}", sd.strides);
        let r: ArrayRef = match tp {
            VarType::F64 => CDFSource::retrieve_f64(&variable, &sd),
            VarType::I64 => CDFSource::retrieve_i64(&variable, &sd),
            VarType::String => CDFSource::retrieve_str(&variable, &sd)
        };

        let schema = self.schema.to_schema();

        Ok(RecordBatch::try_new(schema, vec![r]).unwrap())
    }

    fn variables(&self) -> Vec<String> {
        let mut r = Vec::new();
        for f in self.schema.vars.iter() {
            r.push(f.name.clone());
        }
        r
    }
}

struct ProxySource {
    schema: Schema,
    sources: Vec<Arc<dyn Source>>,
    lookups: BTreeMap<String, usize>
}

impl ProxySource {
    fn new() -> Self {
        Self {
            schema: Schema::new(Vec::new()),
            sources: Vec::new(),
            lookups: BTreeMap::new()
        }
    }

    fn add(&mut self, source: Arc<dyn Source>) {
        let n = self.sources.len();
        for v in source.variables().iter() {
            self.lookups.insert(v.clone(), n);
        }
        self.schema.add_vars(source.schema());
        self.sources.push(source);
    }
}

impl Source for ProxySource {
    fn schema(&self) -> &Schema { &self.schema }

    fn put_into(&mut self, q: &Query, rb: &RecordBatch) -> Result<(), SourcePutError> {
        Ok(())
    }

    fn retrieve(&self, q: &Query) -> Result<RecordBatch, StoreRetrieveError> {
        let index = self.lookups[&q.select];
        self.sources[index].retrieve(q)
    }

    fn variables(&self) -> Vec<String> {
        let mut vs = Vec::new();
        for s in self.sources.iter() {
            for v in s.variables().iter() {
                vs.push(v.clone());
            }
        }
        vs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Float64BufferBuilder, BufferBuilderTrait};

    #[test]
    fn retrieve() {
        let filename = "netcdf-store-retrieve.nc";
        {
            std::fs::remove_file(filename).unwrap_or_default();
            let mut f = netcdf::create(filename).unwrap();
            f.add_dimension("time", 10).unwrap();
            f.add_dimension("y", 5).unwrap();
            f.add_dimension("x", 3).unwrap();
            let mut v = f.add_variable::<f64>("surface_water__depth", &["time", "y", "x"]).unwrap();
            // write all input rainfall data for each raster cell at time t=0
            v.put_values_strided((0..150).into_iter().map(|e| e as f64).collect::<Vec<f64>>().as_slice(),
                                 Some(&[0,0,0]), Some(&[10,5,3]), &[1,1,1]).unwrap();
        }
        let source = CDFSource::try_new(
            filename.to_string(),
            Schema::new(vec![
                Var::new("surface_water__depth".to_string(), VarType::F64),
            ]))
            .unwrap();
        let q = Query::new(
            "surface_water__depth".to_string(),
                  vec![Filter::new("y".to_string(), 0), Filter::new("x".to_string(), 0)]);
        let rb = source.retrieve(&q).unwrap();
        let slice = rb
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let vals = slice.value_slice(0, slice.len());
        assert_eq!(vals, &[0.0, 15.0, 30.0, 45.0, 60.0, 75.0, 90.0, 105.0, 120.0, 135.0])
    }

    #[test]
    fn put() {
        use arrow::datatypes as dt;
        use arrow::array::Float64Array;
        let filename = "netcdf-store-put.nc";
        {
            std::fs::remove_file(filename).unwrap_or_default();
            let mut f = netcdf::create(filename).unwrap();
            f.add_dimension("time", 10).unwrap();
            f.add_dimension("y", 5).unwrap();
            f.add_dimension("x", 3).unwrap();
            let mut v = f.add_variable::<f64>("surface_water__depth", &["time", "y", "x"]).unwrap();
            // write all input rainfall data for each raster cell at time t=0
            v.put_values_strided((0..150).into_iter().map(|e| e as f64).collect::<Vec<f64>>().as_slice(),
                                 Some(&[0,0,0]), Some(&[10,5,3]), &[1,1,1]).unwrap();
        }
        let mut source = CDFSource::try_new(
            filename.to_string(),
            Schema::new(vec![
                Var::new("surface_water__depth".to_string(), VarType::F64),
            ]))
            .unwrap();
        let q = Query::new(
            "surface_water__depth".to_string(),
                  vec![Filter::new("y".to_string(), 0), Filter::new("x".to_string(), 0)]);
        let rb = RecordBatch::try_new(
            Arc::new(dt::Schema::new(
                vec![
                    dt::Field::new(
                        "surface_water__depth",
                        dt::DataType::Float64,
                        false)
                ])),
                vec![
                    {
                        let mut b = Float64Builder::new(10);
                        b.append_slice(&[500f64; 10]);
                        Arc::new(b.finish())
                    }
                ]
        ).unwrap();
        source.put_into(&q, &rb);
        let rb = source.retrieve(&q).unwrap();
        let slice = rb
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let vals = slice.value_slice(0, slice.len());
        assert_eq!(vals, &[500f64; 10])
    }
}