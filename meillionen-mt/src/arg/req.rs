use std::collections::{BTreeMap, HashMap};

use arrow::datatypes::DataType;
use serde_derive::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;

#[typetag::serde(tag = "type")]
pub trait Sink: Debug + Send + Sync {}

#[typetag::serde(tag = "type")]
pub trait Source: Debug + Send + Sync {}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NetCDFResource {
    pub path: String,
    pub variable: String,
    pub data_type: DataType,
    pub slices: HashMap<String, (usize, usize)>,
}

#[typetag::serde]
impl Sink for NetCDFResource {}

#[typetag::serde]
impl Source for NetCDFResource {}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FeatherResource {
    pub path: String,
}

#[typetag::serde]
impl Sink for FeatherResource {}

#[typetag::serde]
impl Source for FeatherResource {}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ParquetResource {
    pub path: String,
}

#[typetag::serde]
impl Sink for ParquetResource {}

#[typetag::serde]
impl Source for ParquetResource {}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FileResource {
    pub path: String,
}

#[typetag::serde]
impl Sink for FileResource {}

#[typetag::serde]
impl Source for FileResource {}

// right now source and sink types are the same
// but that will change when the request handling
// is more established
pub type SinkResource = Arc<dyn Sink>;
pub type SourceResource = Arc<dyn Source>;
pub type SinkResourceMap = BTreeMap<String, SinkResource>;
pub type SourceResourceMap = BTreeMap<String, SourceResource>;
