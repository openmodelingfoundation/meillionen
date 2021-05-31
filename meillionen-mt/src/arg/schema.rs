use arrow::datatypes::{Field};
use crate::{impl_try_from_u8, impl_try_from_validator};
use serde_derive::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TensorSchema {
    pub dimensions: Vec<String>,
    pub data_type: arrow::datatypes::DataType,
    pub resources: Vec<String>,
}

impl_try_from_u8!(TensorSchema);
impl_try_from_validator!(TensorSchema);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Columns {
    fields: Vec<Field>,
}

impl Columns {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataFrameSchema {
    columns: Arc<Columns>,
    description: String,
    resources: Vec<String>,
}

impl DataFrameSchema {
    pub fn new(resources: Vec<String>, description: &str, columns: Arc<Columns>) -> Self {
        Self {
            resources,
            description: description.to_string(),
            columns
        }
    }
}

impl_try_from_u8!(DataFrameSchema);
impl_try_from_validator!(DataFrameSchema);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schemaless {
    ext: String
}

impl Schemaless {
    pub fn new(ext: &str) -> Self {
        Self {
            ext: ext.to_string()
        }
    }
}

impl_try_from_u8!(Schemaless);
impl_try_from_validator!(Schemaless);