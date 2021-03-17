use serde_derive::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct DimMeta {
    pub name: String,
    pub size: usize,
    pub description: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct TensorStackMeta {
    dimensions: Vec<Arc<DimMeta>>,
}

impl TensorStackMeta {
    pub fn new(dimensions: Vec<Arc<DimMeta>>) -> Self {
        Self { dimensions }
    }

    pub fn dimensions(&self) -> &Vec<Arc<DimMeta>> {
        &self.dimensions
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub enum TableMeta {
    TensorStackMeta(Arc<TensorStackMeta>),
}

#[cfg(test)]
mod tests {

    use crate::extension_columns::DimMeta;
    use crate::extension_columns::{TableMeta, TensorStackMeta};
    use std::sync::Arc;

    fn array_meta() {
        let am = TensorStackMeta::new(vec![]);
        assert_eq!(serde_json::to_string(&am).unwrap(), "[]");

        let am = TensorStackMeta::new(vec![Arc::new(DimMeta {
            name: 'x'.to_string(),
            size: 10,
            description: None,
        })]);
        let am_s = r#"[{"name":"x","size":10}]"#;
        assert_eq!(serde_json::to_string(&am).unwrap(), am_s);

        let tm_s = r#"{"TableStackMeta":[{"name":"x","size":10}]}"#;
        let tm = TableMeta::TensorStackMeta(Arc::new(am));
        assert_eq!(serde_json::to_string(&tm).unwrap(), tm_s);
        assert_eq!(serde_json::from_str::<TableMeta>(tm_s).unwrap(), tm);
    }
}
