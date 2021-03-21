pub use validation::ArgValidatorType;
use serde_derive::{Deserialize, Serialize};
use crate::arg::req::{FeatherResource, NetCDFResource};

pub mod req;
pub mod validation;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum ArgResource {
    NetCDF(NetCDFResource),
    Feather(FeatherResource)
}
