use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::mem;
use std::sync::Arc;

use revision::error::Error;
use revision::{Revisioned, revisioned};
use serde::de::Deserializer;
use serde::{Deserialize, Serialize, Serializer};

use crate::kvs::impl_kv_value_revisioned;
use crate::val::Value;

/// Represents a record stored in the database
///
/// `Data` is the type of the data stored in the record
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
pub struct Record {
	pub(crate) metadata: Option<Metadata>,
	pub(crate) data: Data,
}

impl_kv_value_revisioned!(Record);

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

	pub(crate) fn into_read_only(mut self) -> Self {
		if let Data::Mutable(value) = &mut self.data {
			let value = mem::take(value);
			let arc = Arc::new(value);
			self.data = Data::ReadOnly(arc);
		}
		self
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

#[derive(Clone, Debug)]
pub(crate) enum Data {
	// TODO (DB-655): Switch to `Object`.
	Mutable(Value),
	ReadOnly(Arc<Value>),
}

impl Data {
	pub(crate) fn as_ref(&self) -> &Value {
		match self {
			Data::Mutable(value) => value,
			Data::ReadOnly(arc) => arc,
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
		self.as_ref().serialize_revisioned(writer)
	}

	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, Error> {
		Value::deserialize_revisioned(reader).map(Self::Mutable)
	}

	fn revision() -> u16 {
		1
	}
}

impl PartialEq for Data {
	fn eq(&self, other: &Self) -> bool {
		self.as_ref() == other.as_ref()
	}
}

impl PartialOrd for Data {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		self.as_ref().partial_cmp(other.as_ref())
	}
}

impl Hash for Data {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.as_ref().hash(state);
	}
}

impl Serialize for Data {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		self.as_ref().serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for Data {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		Value::deserialize(deserializer).map(Self::Mutable)
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
