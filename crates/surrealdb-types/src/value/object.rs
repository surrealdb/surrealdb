use std::collections::BTreeMap;

use crate::Value;

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Object(pub BTreeMap<String, Value>);