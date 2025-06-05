use crate::sql::dir::Dir;
use crate::sql::table::Tables;
use crate::sql::thing::Thing;
use anyhow::Result;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use super::graph::GraphSubjects;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Edges";

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Edges")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Edges {
	pub dir: Dir,
	pub from: Thing,
	#[revision(end = 2, convert_fn = "convert_old_what")]
	pub _what: Tables,
	#[revision(start = 2)]
	pub what: GraphSubjects,
}

impl Edges {
	pub fn new(dir: Dir, from: Thing, what: GraphSubjects) -> Self {
		Edges {
			dir,
			from,
			what,
		}
	}

	fn convert_old_what(&mut self, _rev: u16, old: Tables) -> Result<(), revision::Error> {
		self.what = old.into();
		Ok(())
	}
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

impl From<Edges> for crate::expr::Edges {
	fn from(v: Edges) -> Self {
		Self {
			dir: v.dir.into(),
			from: v.from.into(),
			what: v.what.into(),
		}
	}
}

impl From<crate::expr::Edges> for Edges {
	fn from(v: crate::expr::Edges) -> Self {
		Self {
			dir: v.dir.into(),
			from: v.from.into(),
			what: v.what.into(),
		}
	}
}
