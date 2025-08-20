use crate::Value;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Array(pub Vec<Value>);