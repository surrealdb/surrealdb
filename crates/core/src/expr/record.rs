use crate::expr::Object;
use revision::revisioned;
use serde::Deserialize;
use serde::Serialize;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub(crate) struct Record {
	metadata: Option<Metadata>,
	data: Object,
}

impl Record {
	pub fn is_edge(&self) -> bool {
		match &self.metadata {
			Some(Metadata {
				record_type: Some(RecordType::Edge),
				..
			}) => true,
			_ => false,
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub(crate) struct Metadata {
	record_type: Option<RecordType>,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub(crate) enum RecordType {
	Edge,
}
