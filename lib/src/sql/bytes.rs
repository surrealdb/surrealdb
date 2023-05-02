use serde::{Deserialize, Serialize};
use std::ops::Deref;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
pub struct Bytes(#[serde(with = "serde_bytes")] pub(crate) Vec<u8>);

impl Deref for Bytes {
	type Target = Vec<u8>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

#[cfg(test)]
mod tests {
	use crate::sql::{Bytes, Value};

	#[test]
	fn serialize() {
		let val = Value::Bytes(Bytes(vec![1, 2, 3, 5]));
		let serialized: Vec<u8> = val.into();
		println!("{serialized:?}");
		let deserialized = Value::from(serialized);
		println!("{deserialized:?}");
	}
}
