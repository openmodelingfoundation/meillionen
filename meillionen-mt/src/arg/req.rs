use arrow::datatypes::{DataType};
use serde_derive::{Deserialize, Serialize};

use std::collections::HashMap;

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
