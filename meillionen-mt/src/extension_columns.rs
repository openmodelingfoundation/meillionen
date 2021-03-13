use serde_derive::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DimMeta {
    name: String,
    size: u64,
    description: Option<String>
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TensorStackMeta {
    dimensions: Vec<DimMeta>
}

impl TensorStackMeta {
    pub fn new(dimensions: Vec<DimMeta>) -> Self {
        Self {
            dimensions
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub enum TableMeta {
    TensorStackMeta(Arc<TensorStackMeta>)
}

#[cfg(test)]
mod tests {
    use serde_json;
    use crate::extension_columns::TensorStackMeta;
    use crate::extension_columns::DimMeta;

    fn array_meta() {
        let am = TensorStackMeta::new(vec![]);
        assert_eq!(serde_json::to_string(&am).unwrap(), "[]");

        let am = TensorStackMeta::new(vec![
            DimMeta {
                name: 'x'.to_string(),
                size: 10,
                description: None
            },
        ]);
        assert_eq!(serde_json::to_string(&am).unwrap(), r#"[{"name":"x","size":10}]"#)
    }
}