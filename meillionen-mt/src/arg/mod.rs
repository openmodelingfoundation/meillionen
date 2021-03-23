use serde_derive::{Deserialize, Serialize};

pub use validation::ArgValidatorType;

use crate::arg::req::{FeatherResource, NetCDFResource};

pub mod req;
pub mod validation;