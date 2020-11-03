use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use netcdf::Variable;
use arrow::datatypes;
use arrow::array::{ArrayRef, Int64Array, Int64Builder, Float64Builder, Float64Array};
use thiserror::Error;
use std::convert::TryFrom;
use std::collections::BTreeMap;
use std::fmt::Display;

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
    fn put_into(&mut self, q: &Query, rb: &RecordBatch) -> Option<String>;
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

pub struct CDFSource {
    pub schema: Schema,
    pub file: Arc<netcdf::File>
}

impl CDFSource {
    pub fn try_new(path: String, schema: Schema) -> netcdf::error::Result<Self> {
        let file = Arc::new(netcdf::open(path)?);
        Ok(Self {
            schema,
            file
        })
    }

    fn retrieve_f64(variable: &netcdf::Variable, capacity: usize, indices: &[usize], slice_len: &[usize], strides: &[isize]) -> ArrayRef {
        let mut b = vec![0f64; capacity];
        let n = slice_len.len();

        variable.values_strided_to(b.as_mut_slice(), Some(indices), Some(slice_len), strides).unwrap();

        let mut res = Float64Builder::new(n);
        res.append_slice(b.as_slice());
        Arc::new(res.finish())
    }

    fn retrieve_i64(variable: &netcdf::Variable, capacity: usize, indices: &[usize], slice_len: &[usize], strides: &[isize]) -> ArrayRef {
        let mut b = vec![0i64; capacity];
        let n = slice_len.len();

        variable.values_strided_to(b.as_mut_slice(), Some(indices), Some(slice_len), strides).unwrap();

        let mut res= Int64Builder::new(n);
        res.append_slice(b.as_slice());
        Arc::new(res.finish())
    }

    fn retrieve_str(variable: &netcdf::Variable, capacity: usize, indices: &[usize], slice_len: &[usize], strides: &[isize]) -> ArrayRef {
        let mut b = arrow::array::StringBuilder::new(capacity);
        // let n = slice_len.len();
        //
        // variable.values_strided_to(b.into(), Some(indices), Some(slice_len), strides).unwrap();
        Arc::new(b.finish())
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

    fn put_into(&mut self, q: &Query, rb: &RecordBatch) -> Option<String> {
        let s = self.schema();
        let schema = rb.schema();
        for f in schema.fields() {
            let tp = s.get_type(f.name());

        }

        None
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

        println!("indices: {:?}", indices);
        println!("slice_len: {:?}", slice_len);
        println!("strides: {:?}", strides);
        let r: ArrayRef = match tp {
            VarType::F64 => CDFSource::retrieve_f64(&variable, capacity, indices.as_slice(), slice_len.as_slice(), strides.as_slice()),
            VarType::I64 => CDFSource::retrieve_i64(&variable, capacity, indices.as_slice(), slice_len.as_slice(), strides.as_slice()),
            VarType::String => CDFSource::retrieve_str(&variable, capacity, indices.as_slice(), slice_len.as_slice(), strides.as_slice())
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

    fn put_into(&mut self, q: &Query, rb: &RecordBatch) -> Option<String> {
        None
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
    use arrow::array::Float64Array;

    #[test]
    fn slice() {
        let filename = "netcdf-store-ex.nc";
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
}