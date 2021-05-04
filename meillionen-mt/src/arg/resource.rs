use crate::{impl_try_from_u8, impl_try_from_validator};
use serde_derive::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NetCDFResource {
    pub path: String,
    pub variable: String,
    pub dimensions: Vec<String>,
}

impl_try_from_u8!(NetCDFResource);
impl_try_from_validator!(NetCDFResource);

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FeatherResource {
    pub path: String,
}

impl_try_from_u8!(FeatherResource);
impl_try_from_validator!(FeatherResource);

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ParquetResource {
    pub path: String,
}

impl_try_from_u8!(ParquetResource);
impl_try_from_validator!(ParquetResource);

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FileResource {
    pub path: String,
}

impl_try_from_u8!(FileResource);
impl_try_from_validator!(FileResource);
