use crate::dir::Dir;
use crate::table::Tables;
use crate::thing::Thing;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

pub(crate) const TOKEN: &str = "$surrealdb::private::crate::Edges";

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::crate::Edges")]
#[revisioned(revision = 1)]
pub struct Edges {
	pub dir: Dir,
	pub from: Thing,
	pub what: Tables,
}

impl fmt::Display for Edges {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self.what.len() {
			0 => write!(f, "{}{}?", self.from, self.dir,),
			1 => write!(f, "{}{}{}", self.from, self.dir, self.what),
			_ => write!(f, "{}{}({})", self.from, self.dir, self.what),
		}
	}
}
