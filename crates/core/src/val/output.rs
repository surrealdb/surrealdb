use std::collections::BTreeMap;

use crate::expr::Kind;
use crate::val::Value;
use crate::val::record::Record;
use crate::val::value::CoerceError;
use crate::val::value::CastError;

#[derive(Clone, Debug)]
pub enum Output {
	/// A single record from the database
	Record(Record),
	/// A single value (could be any Value type)
	Value(Value),
	/// An array of output items (recursive)
	Array(Vec<Output>),
	Map(BTreeMap<String, Output>),
}

impl Output {
	pub(crate) fn into_value(self) -> Value {
		match self {
			Output::Record(record) => record.data.into_value(),
			Output::Value(value) => value,
			Output::Array(array) => array.into_iter().map(Self::into_value).collect(),
			Output::Map(map) => map.into_iter().map(|(k, v)| (k, v.into_value())).collect(),
		}
	}

	pub(crate) fn is_truthy(&self) -> bool {
		match self {
			Output::Record(record) => {
				!record.metadata.is_some() && record.data.as_ref().is_truthy()
			}
			Output::Value(value) => value.is_truthy(),
			Output::Array(array) => array.iter().any(Self::is_truthy),
			Output::Map(map) => map.values().any(Self::is_truthy),
		}
	}

	pub(crate) fn coerce_to_kind(self, kind: &Kind) -> Result<Value, CoerceError> {
		match self {
			Output::Record(record) => record.data.into_value().coerce_to_kind(kind),
			Output::Value(value) => value.coerce_to_kind(kind),
			Output::Array(array) => {
				array.into_iter().map(|output| output.coerce_to_kind(kind)).collect()
			}
			Output::Map(map) => {
				let mut res = BTreeMap::new();
				for (key, value) in map.into_iter() {
					res.insert(key, value.coerce_to_kind(kind)?);
				}
				Ok(res.into())
			}
		}
	}

    pub(crate) fn cast_to_kind(self, kind: &Kind) -> Result<Value, CastError> {
		match self {
			Output::Record(record) => record.data.into_value().cast_to_kind(kind),
			Output::Value(value) => value.cast_to_kind(kind),
			Output::Array(array) => array.into_iter().map(|output| output.cast_to_kind(kind)).collect(),
			Output::Map(map) => {
                let mut res = BTreeMap::new();
                for (key, value) in map.into_iter() {
                    res.insert(key, value.cast_to_kind(kind)?);
                }
                Ok(res.into())
            },
		}
	}

    pub(crate) fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Output::Record(record) => if let Value::Object(obj) = record.data.as_ref() && Some(value) = obj.get(key) {
                return Some(value);
            },
            Output::Value(value) => if let Value::Object(obj) = value && Some(value) = obj.get(key) {
                return Some(value);
            },
            Output::Array(array) => {},
            Output::Map(map) => if let Some(output) = map.get(key) {
                match output {
                    Output::Value(value) => value,
                    Output::Record(record) => record.data.as_ref(),
                    _ => {}
            }
        }
        None
    }
}
