use std::mem;
use std::sync::Arc;

use revision::error::Error;
use revision::{Revisioned, revisioned};
use serde::{Deserialize, Serialize};

use crate::val::Value;

/// Represents a record stored in the database
///
/// `Data` is the type of the data stored in the record
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct Record {
	pub(crate) metadata: Option<Metadata>,
	// TODO (DB-655): Switch to `Object`.
	pub(crate) data: Data,
}

impl Record {
	pub(crate) fn new(data: Data) -> Self {
		Self {
			metadata: None,
			data,
		}
	}

	pub const fn is_edge(&self) -> bool {
		matches!(
			&self.metadata,
			Some(Metadata {
				record_type: Some(RecordType::Edge),
				..
			})
		)
	}

	pub(crate) fn set_record_type(&mut self, rtype: RecordType) {
		match &mut self.metadata {
			Some(metadata) => {
				metadata.record_type = Some(rtype);
			}
			metadata => {
				*metadata = Some(Metadata {
					record_type: Some(rtype),
				});
			}
		}
	}
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(untagged)]
pub(crate) enum Data {
	Mutable(Value),
	ReadOnly(Arc<Value>),
}

impl Data {
	pub(crate) fn as_ref(&self) -> &Value {
		match self {
			Data::Mutable(value) => value,
			Data::ReadOnly(arc) => arc.as_ref(),
		}
	}

	pub(crate) fn to_mut(&mut self) -> &mut Value {
		match self {
			Data::Mutable(value) => value,
			Data::ReadOnly(arc) => Arc::make_mut(arc),
		}
	}

	pub(crate) fn read_only(&mut self) -> Arc<Value> {
		match self {
			Data::ReadOnly(arc) => arc.clone(),
			Data::Mutable(value) => {
				let value = mem::take(value);
				let arc = Arc::new(value);
				*self = Data::ReadOnly(arc.clone());
				arc
			}
		}
	}
}

impl Default for Data {
	fn default() -> Self {
		Self::Mutable(Value::default())
	}
}

impl Revisioned for Data {
	#[inline]
	fn serialize_revisioned<W: std::io::Write>(&self, writer: &mut W) -> Result<(), Error> {
		match self {
			Data::Mutable(v) => v.serialize_revisioned(writer),
			Data::ReadOnly(v) => v.serialize_revisioned(writer),
		}
	}

	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, Error> {
		Value::deserialize_revisioned(reader).map(Self::Mutable)
	}

	fn revision() -> u16 {
		1
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) struct Metadata {
	record_type: Option<RecordType>,
}

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub(crate) enum RecordType {
	Edge,
}
