use meillionen_mt::{Variable, Dimension, SliceType};
use std::marker::PhantomData;
use ndarray::{ArrayD, Array1, Ix1, SliceInfo};
use itertools::Itertools;
use std::ops::{Range, Index};
use std::convert::TryFrom;

pub struct CDFStoreVariable<'a, T: 'a> {
    name: String,
    variable: netcdf::Variable<'a>,
    pd: PhantomData<T>
}

impl<'a, T> CDFStoreVariable<'a, T> {
    pub fn new(name: impl AsRef<str>, variable: netcdf::Variable<'a>) -> CDFStoreVariable<T> {
        Self {
            name: name.as_ref().to_string(),
            variable,
            pd: PhantomData
        }
    }
}

impl<'a, T> Variable for CDFStoreVariable<'a, T> where T: Default + netcdf::Numeric + Clone + num_traits::identities::Zero {
    type Output = T;

    fn name(&self) -> String {
        self.name.clone()
    }

    fn slice<SI>(&self, index: SI) -> Array1<Self::Output> where SI: AsRef<[SliceType]> {
        let index = index.as_ref();
        assert_eq!(index.len(), self.variable.dimensions().len());
        let size: usize = index.iter()
            .zip(self.variable.dimensions())
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
            .zip(self.variable.dimensions())
            .map(|(ind, dim)| match ind {
                SliceType::Index(_) => 1,
                SliceType::All => dim.len()
            }).collect::<Vec<usize>>();
        let mut buffer = Array1::<T>::zeros(size).to_vec();

        let strides = (0isize..TryFrom::try_from(index.len()).unwrap()).collect::<Vec<isize>>();
        self.variable.values_strided_to(
            buffer.as_mut(),
            Some(indices.as_slice()),
            Some(slice_len.as_slice()),
            strides.as_slice());
        Array1::from(buffer)
    }

    fn get_dimensions(&self) -> Vec<Dimension> {
        self.variable.dimensions().iter().map(|d| Dimension::new(d.name(), d.len())).collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::data::{CDFStoreVariable};
    use meillionen_mt::Variable;
    use pyo3::pyclass::PyClassAlloc;

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

    // #[test]
    // fn dimension_iteration() {
    //     {
    //         std::fs::remove_file("netcdf-dim-iter.nc").unwrap_or_default();
    //         let mut f = netcdf::create("netcdf-dim-iter.nc").unwrap();
    //         f.add_dimension("time", 10).unwrap();
    //         f.add_dimension("y", 5).unwrap();
    //         f.add_dimension("x", 3).unwrap();
    //         let mut v = f.add_variable::<f32>("surface_water__depth", &["time", "y", "x"]).unwrap();
    //         // write all input rainfall data for each raster cell at time t=0
    //         v.put_values_strided((0..150).into_iter().map(|e| e as f32).collect::<Vec<f32>>().as_slice(), Some(&[0,0,0]), Some(&[10,5,3]), &[1,1,1]).unwrap();
    //     }
    //     let f = netcdf::open("netcdf-dim-iter.nc").unwrap();
    //     let var = f.variable("surface_water__depth").unwrap();
    //     let cdfvar = CDFStoreVariable::new("surface_water__depth", var);
    //     let iter = iter_by_dimensions(&cdfvar, &["x", "y"]).unwrap();
    //     assert_eq!(iter.next(), Array1::from(&[0, 15, 30, 45, 60, 75, 90, 105, 120, 135, 150, 165, 180, 195, 210]));
    // }
}