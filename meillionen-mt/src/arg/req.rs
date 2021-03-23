use std::collections::HashMap;

use arrow::datatypes::DataType;
use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NetCDFResource {
    pub path: String,
    pub variable: String,
    pub data_type: DataType,
    pub slices: HashMap<String, (usize, usize)>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FeatherResource {
    pub path: String
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum Resource {
    NetCDF(NetCDFResource),
    Feather(FeatherResource)
}

// right now source and sink types are the same
// but that will change when the request handling
// is more established
pub type SinkResource = Resource;
pub type SourceResource = Resource;
