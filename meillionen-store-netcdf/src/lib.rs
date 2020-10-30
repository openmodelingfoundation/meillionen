use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use netcdf::Variable;
use arrow::datatypes;
use arrow::array::{ArrayRef, Int64Array, Int64Builder, Float64Builder, Float64Array};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StoreRetrieveError {
    #[error("dimension mismatch, expected {expected} but received {received}")]
    DimMismatch {
        expected: usize,
        received: usize
    },
    #[error("variable {0} not found")]
    NotFound(String)
}

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
}

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
}

trait Store {
    fn schema(&self) -> &Schema;
    fn put_into(&mut self, q: &Query, rb: &RecordBatch) -> Option<String>;
    fn retrieve(&self, q: &Query) -> Result<RecordBatch, StoreRetrieveError>;
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

pub struct CDFStore {
    pub schema: Schema,
    pub file: Arc<netcdf::File>
}

impl CDFStore {
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
}

impl Store for CDFStore {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn put_into(&mut self, q: &Query, rb: &RecordBatch) -> Option<String> {
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
            VarType::F64 => CDFStore::retrieve_f64(&variable, capacity, indices.as_slice(), slice_len.as_slice(), strides.as_slice()),
            VarType::I64 => CDFStore::retrieve_i64(&variable, capacity, indices.as_slice(), slice_len.as_slice(), strides.as_slice()),
            VarType::String => CDFStore::retrieve_str(&variable, capacity, indices.as_slice(), slice_len.as_slice(), strides.as_slice())
        };

        let schema = self.schema.to_schema();

        Ok(RecordBatch::try_new(schema, vec![r]).unwrap())
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
        let store = CDFStore::try_new(
            filename.to_string(),
            Schema::new(vec![
                Var::new("surface_water__depth".to_string(), VarType::F64),
            ]))
            .unwrap();
        let q = Query::new(
            "surface_water__depth".to_string(),
                  vec![Filter::new("y".to_string(), 0), Filter::new("x".to_string(), 0)]);
        let rb = store.retrieve(&q).unwrap();
        let slice = rb
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let vals = slice.value_slice(0, slice.len());
        assert_eq!(vals, &[0.0, 15.0, 30.0, 45.0, 60.0, 75.0, 90.0, 105.0, 120.0, 135.0])
    }
}