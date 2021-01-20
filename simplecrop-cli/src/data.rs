use meillionen_mt::{Variable, Dimension, SliceType};
use pyo3::prelude::*;

use ndarray::{ArrayD, Array1, Ix1, SliceInfo};


use std::convert::TryFrom;
use std::path::Path;

use pyo3::exceptions;
use std::sync::Arc;


#[pyclass]
#[derive(Clone)]
pub struct F64CDFVariableRef {
    store: Arc<netcdf::File>,
    variable_name: String
}

impl F64CDFVariableRef {
    pub fn try_new(file: Arc<netcdf::File>, variable_name: impl AsRef<str>) -> eyre::Result<Self> {
        if let None = file.variable(variable_name.as_ref()) {
            return Err(eyre::eyre!("variable {} not found", variable_name.as_ref()))
        }
        Ok(Self {
            store: file,
            variable_name: variable_name.as_ref().to_string()
        })
    }

    pub fn variable(&self) -> netcdf::Variable {
        self.store.variable(self.variable_name.as_str()).unwrap()
    }
}

#[pymethods]
impl F64CDFVariableRef {
    #[getter]
    pub fn name(&self) -> PyResult<String> {
        Ok(self.store.variable(self.variable_name.as_str())
            .unwrap().name())
    }

    #[getter]
    pub fn len(&self) -> PyResult<usize> {
        Ok(self.store.variable(self.variable_name.as_str())
            .unwrap().len())
    }
}

#[pyclass]
#[derive(Clone)]
pub struct CDFStore {
    pub file: Arc<netcdf::File>
}

impl TryFrom<&Path> for CDFStore {
    type Error = eyre::Report;

    fn try_from(value: &Path) -> Result<Self, Self::Error> {
        let file = Arc::new(netcdf::open(value)
            .map_err(|e| eyre::eyre!("CDFStore {}: {}", value.to_str().unwrap(), e.to_string()))?);
        Ok(Self {
            file
        })
    }
}

impl CDFStore {
    pub fn get_f64(&self, variable_name: impl AsRef<str>) -> eyre::Result<F64CDFVariableRef> {
        F64CDFVariableRef::try_new(self.file.clone(), variable_name)
    }
}

#[pymethods]
impl CDFStore {
    #[new]
    pub fn __init__(s: &str) -> PyResult<Self> {
        Self::try_from(Path::new(s)).map_err(|e| exceptions::PyIOError::new_err(e.to_string()))
    }

    pub fn get_f64_variable(self_: PyRef<CDFStore>, variable_name: String) -> PyResult<F64CDFVariableRef> {
        self_.get_f64(variable_name)
            .map_err(|e| exceptions::PyKeyError::new_err(e.to_string()))
    }
}

impl Variable for F64CDFVariableRef {
    type Elem = f64;
    type Index = Vec<SliceType>;

    fn name(&self) -> String {
        self.variable_name.clone()
    }

    fn slice(&self, index: &Self::Index) -> Array1<Self::Elem> {
        let variable = self.variable();
        assert_eq!(index.len(), variable.dimensions().len());
        let size: usize = index.iter()
            .zip(self.variable().dimensions())
            .map(|(ind, dim)| match ind {
                SliceType::Index(_) => 1,
                SliceType::All => dim.len()
            })
            .product();

        let indices = index.iter().map(|ind| match ind {
            SliceType::Index(ref x) => x.clone(),
            SliceType::All => 0
        }).collect::<Vec<usize>>();

        let slice_len = index.iter()
            .zip(variable.dimensions())
            .map(|(ind, dim)| match ind {
                SliceType::Index(_) => 1,
                SliceType::All => dim.len()
            }).collect::<Vec<usize>>();
        let mut buffer = Array1::<Self::Elem>::zeros(size).to_vec();

        let strides = Array1::<isize>::ones(index.len()).to_vec();
        variable.values_strided_to(
            buffer.as_mut(),
            Some(indices.as_slice()),
            Some(slice_len.as_slice()),
            strides.as_slice());
        Array1::from(buffer)
    }

    fn get_dimensions(&self) -> Vec<Dimension> {
        self.variable().dimensions().iter().map(|d| Dimension::new(d.name(), d.len())).collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::data::{CDFStore};
    use ndarray::Array1;
    use meillionen_mt::{Variable, SliceType};
    use pyo3::pyclass::PyClassAlloc;
    use std::convert::TryFrom;
    use std::path::Path;

    #[test]
    fn store_variable_access() {
        {
            std::fs::remove_file("netcdf-var-access.nc").unwrap_or_default();
            let mut f = netcdf::create("netcdf-var-access.nc").unwrap();
            f.add_dimension("time", 10).unwrap();
            f.add_dimension("y", 5).unwrap();
            f.add_dimension("x", 3).unwrap();
            let mut v = f.add_variable::<f32>("surface_water__depth", &["time", "y", "x"]).unwrap();
            // write all input rainfall data for each raster cell at time t=0
            v.put_values_strided((0..10).into_iter().map(|e| e as f32).collect::<Vec<f32>>().as_slice(), Some(&[0,0,0]), Some(&[10,1,1]), &[1,1,1]).unwrap();
        }
    }

    #[test]
    fn dimension_iteration() {
        {
            std::fs::remove_file("netcdf-dim-iter.nc").unwrap_or_default();
            let mut f = netcdf::create("netcdf-dim-iter.nc").unwrap();
            f.add_dimension("time", 10).unwrap();
            f.add_dimension("y", 5).unwrap();
            f.add_dimension("x", 3).unwrap();
            let mut v = f.add_variable::<f64>("surface_water__depth", &["time", "y", "x"]).unwrap();
            // write all input rainfall data for each raster cell at time t=0
            v.put_values_strided((0..150).into_iter().map(|e| e as f64).collect::<Vec<f64>>().as_slice(), Some(&[0,0,0]), Some(&[10,5,3]), &[1,1,1]).unwrap();
        }
        let store = CDFStore::try_from(Path::new("netcdf-dim-iter.nc")).unwrap();
        let var = store.get_f64("surface_water__depth").unwrap();
        let arr = var.slice(&vec![SliceType::All, SliceType::Index(0), SliceType::Index(0)]);
        assert_eq!(arr, Array1::from(vec![0.0, 15.0, 30.0, 45.0, 60.0, 75.0, 90.0, 105.0, 120.0, 135.0]));
    }
}