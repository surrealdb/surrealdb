use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Record {
	pub(crate) metadata: Option<Metadata>,
	// TODO (DB-655): Switch to `Object`.
	pub(crate) data: Value,
}

impl Record {
	pub(crate) fn new(data: Value) -> Self {
		Self {
			metadata: None,
			data,
		}
	}

	pub const fn is_edge(&self) -> bool {
		match &self.metadata {
			Some(Metadata {
				record_type: Some(RecordType::Edge),
				..
			}) => true,
			_ => false,
		}
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct Metadata {
	record_type: Option<RecordType>,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum RecordType {
	Edge,
}
