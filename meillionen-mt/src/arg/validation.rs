use arrow::datatypes::{Field};
use crate::{impl_try_from_u8, impl_try_from_validator};
use serde_derive::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TensorValidator {
    pub dimensions: Vec<String>,
    pub data_type: arrow::datatypes::DataType,
    pub resources: Vec<String>,
}

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
pub struct DataFrameValidator {
    columns: Arc<Columns>,
    description: String,
    resources: Vec<String>,
}

impl DataFrameValidator {
    pub fn new(resources: Vec<String>, description: &str, columns: Arc<Columns>) -> Self {
        Self {
            resources,
            description: description.to_string(),
            columns
        }
    }
}

impl_try_from_u8!(DataFrameValidator);
impl_try_from_validator!(DataFrameValidator);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Unvalidated {}

impl Unvalidated {
    pub fn new() -> Self {
        Self {}
    }
}

impl_try_from_u8!(Unvalidated);
impl_try_from_validator!(Unvalidated);