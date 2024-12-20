use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::{ctx::Context, dbs::Options, doc::CursorDoc, err::Error};

use super::{Idiom, Table, Value};

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
		write!(f, "REFERENCE ON DELETE {}", &self.on_delete)
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
	WipeValue,
	Custom(Value),
}

impl fmt::Display for ReferenceDeleteStrategy {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			ReferenceDeleteStrategy::Reject => write!(f, "REJECT"),
			ReferenceDeleteStrategy::Ignore => write!(f, "IGNORE"),
			ReferenceDeleteStrategy::Cascade => write!(f, "CASCADE"),
			ReferenceDeleteStrategy::WipeValue => write!(f, "WIPE VALUE"),
			ReferenceDeleteStrategy::Custom(v) => write!(f, "THEN {}", v),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, PartialOrd)]
#[serde(rename = "$surrealdb::private::sql::Range")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Refs(pub Option<Table>, pub Option<Idiom>);

impl Refs {
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		let arr = match doc {
			Some(doc) => match &doc.rid {
				Some(id) => {
					let ids = id.refs(ctx, opt, self.0.as_ref(), self.1.as_ref()).await?;
					ids.into_iter().map(Value::Thing).collect()
				}
				None => return Err(Error::Unreachable("bla".into())),
			},
			None => return Err(Error::Unreachable("bla".into())),
		};

		Ok(Value::Array(arr))
	}
}

impl fmt::Display for Refs {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "[]")
	}
}
