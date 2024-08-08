use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::{escape::escape_rid, id::Id, Strand, Value};
use crate::syn;
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Thing";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[serde(rename = "$surrealdb::private::sql::Thing")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Thing {
	/// Table name
	pub tb: String,
	pub id: Id,
}

impl From<(&str, Id)> for Thing {
	fn from((tb, id): (&str, Id)) -> Self {
		Self {
			tb: tb.to_owned(),
			id,
		}
	}
}

impl From<(String, Id)> for Thing {
	fn from((tb, id): (String, Id)) -> Self {
		Self {
			tb,
			id,
		}
	}
}

impl From<(String, String)> for Thing {
	fn from((tb, id): (String, String)) -> Self {
		Self::from((tb, Id::from(id)))
	}
}

impl From<(&str, &str)> for Thing {
	fn from((tb, id): (&str, &str)) -> Self {
		Self::from((tb.to_owned(), Id::from(id)))
	}
}

impl FromStr for Thing {
	type Err = ();
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Self::try_from(s)
	}
}

impl TryFrom<String> for Thing {
	type Error = ();
	fn try_from(v: String) -> Result<Self, Self::Error> {
		Self::try_from(v.as_str())
	}
}

impl TryFrom<Strand> for Thing {
	type Error = ();
	fn try_from(v: Strand) -> Result<Self, Self::Error> {
		Self::try_from(v.as_str())
	}
}

impl TryFrom<&str> for Thing {
	type Error = ();
	fn try_from(v: &str) -> Result<Self, Self::Error> {
		match syn::thing(v) {
			Ok(v) => Ok(v),
			_ => Err(()),
		}
	}
}

impl Thing {
	/// Convert the Thing to a raw String
	pub fn to_raw(&self) -> String {
		self.to_string()
	}
}

impl fmt::Display for Thing {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:{}", escape_rid(&self.tb), self.id)
	}
}

impl Thing {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		Ok(Value::Thing(Thing {
			tb: self.tb.clone(),
			id: self.id.compute(stk, ctx, opt, doc).await?,
		}))
	}
}
