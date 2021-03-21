use std::sync::Arc;
use arrow::datatypes::Schema;
use serde_derive::{Serialize, Deserialize};
use serde_json::{Value as JsonValue};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TensorValidator {
    pub dimensions: Vec<String>,
    pub data_type: arrow::datatypes::DataType
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataFrameValidator(pub Arc<Schema>);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ArgValidatorType {
    Tensor(Arc<TensorValidator>),
    DataFrame(Arc<DataFrameValidator>)
}

impl ArgValidatorType {
    pub fn new_tensor(tv: Arc<TensorValidator>) -> Self {
        Self::Tensor(tv)
    }

    pub fn new_dataframe(df: Arc<DataFrameValidator>) -> Self {
        Self::DataFrame(df)
    }

    pub fn get_tensor(&self) -> Option<Arc<TensorValidator>> {
        if let Self::Tensor(ref tv) = self {
            return Some(tv.clone())
        }
        None
    }

    pub fn get_dataframe(&self) -> Option<Arc<DataFrameValidator>> {
        if let Self::DataFrame(ref df) = self {
            return Some(df.clone())
        }
        None
    }
}