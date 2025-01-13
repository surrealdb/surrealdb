use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::{cnf::EXPERIMENTAL_RECORD_REFERENCES, ctx::Context, dbs::Options, doc::CursorDoc, err::Error};

use super::{array::Uniq, statements::info::InfoStructure, Array, Idiom, Table, Thing, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, PartialOrd)]
#[serde(rename = "$surrealdb::private::sql::Range")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Reference {
	pub on_delete: ReferenceDeleteStrategy,
}

impl fmt::Display for Reference {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ON DELETE {}", &self.on_delete)
	}
}

impl InfoStructure for Reference {
	fn structure(self) -> Value {
		map! {
			"on_delete" => self.on_delete.structure(),
		}
		.into()
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, PartialOrd)]
#[serde(rename = "$surrealdb::private::sql::Range")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum ReferenceDeleteStrategy {
	Reject,
	Ignore,
	Cascade,
	Unset,
	Custom(Value),
}

impl fmt::Display for ReferenceDeleteStrategy {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			ReferenceDeleteStrategy::Reject => write!(f, "REJECT"),
			ReferenceDeleteStrategy::Ignore => write!(f, "IGNORE"),
			ReferenceDeleteStrategy::Cascade => write!(f, "CASCADE"),
			ReferenceDeleteStrategy::Unset => write!(f, "UNSET"),
			ReferenceDeleteStrategy::Custom(v) => write!(f, "THEN {}", v),
		}
	}
}

impl InfoStructure for ReferenceDeleteStrategy {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, PartialOrd)]
#[serde(rename = "$surrealdb::private::sql::Range")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Refs(pub Vec<(Option<Table>, Option<Idiom>)>);

impl Refs {
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		if !*EXPERIMENTAL_RECORD_REFERENCES {
			return Ok(Value::Array(Default::default()))
		}

		// Collect an array of references
		let arr: Array = match doc {
			// Check if the current document has specified an ID
			Some(doc) => {
				// Obtain a record id from the document
				let rid = match &doc.rid {
					Some(id) => id.as_ref().to_owned(),
					None => match &doc.doc.rid() {
						Value::Thing(id) => id.to_owned(),
						_ => return Err(Error::InvalidRefsContext),
					},
				};

				let mut ids: Vec<Thing> = Vec::new();

				// Map over all input pairs
				for (ft, ff) in self.0.iter() {
					// Collect the references
					ids.append(&mut rid.refs(ctx, opt, ft.as_ref(), ff.as_ref()).await?);
				}

				// Convert the references into values
				ids.into_iter().map(Value::Thing).collect()
			}
			None => return Err(Error::InvalidRefsContext),
		};

		Ok(Value::Array(arr.uniq()))
	}
}

impl fmt::Display for Refs {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "[]")
	}
}
