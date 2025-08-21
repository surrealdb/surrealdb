use serde::{Deserialize, Serialize};

use crate::Value;

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Array(pub Vec<Value>);
