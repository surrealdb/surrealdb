use crate::expr::Thing;
use crate::expr::Value;
use revision::revisioned;
use serde::Deserialize;
use serde::Serialize;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Record {
	pub(crate) metadata: Option<Metadata>,
	// TODO (DB-655): Switch to `Object`.
	pub(crate) data: Value,
}

impl Record {
	pub const fn is_edge(&self) -> bool {
		match &self.metadata {
			Some(Metadata {
				record_type: Some(RecordType::Edge {
					..
				}),
				..
			}) => true,
			_ => false,
		}
	}

	pub(crate) fn update_edges(&mut self, incoming: Thing, outgoing: Thing) {
		let rtype = Some(RecordType::Edge {
			incoming,
			outgoing,
		});
		match &mut self.metadata {
			Some(Metadata {
				record_type,
				..
			}) => {
				*record_type = rtype;
			}
			metadata => {
				*metadata = Some(Metadata {
					record_type: rtype,
				});
			}
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
	Edge {
		incoming: Thing,
		outgoing: Thing,
	},
}
