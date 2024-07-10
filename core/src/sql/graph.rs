use std::fmt::{self, Display, Formatter, Write};

use crate::sql::dir::Dir;
use crate::sql::field::Fields;
use crate::sql::group::Groups;
use crate::sql::idiom::Idiom;
use crate::sql::limit::Limit;
use crate::sql::order::Orders;
use crate::sql::split::Splits;
use crate::sql::start::Start;
use crate::sql::table::Tables;
use crate::sql::Cond;
use revision::revisioned;
use serde::{Deserialize, Serialize};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Graph {
	pub dir: Dir,
	pub expr: Fields,
	pub what: Tables,
	pub cond: Option<Cond>,
	pub split: Option<Splits>,
	pub group: Option<Groups>,
	pub order: Option<Orders>,
	pub limit: Option<Limit>,
	pub start: Option<Start>,
	pub alias: Option<Idiom>,
}

impl Graph {
	/// Convert the graph edge to a raw String
	pub fn to_raw(&self) -> String {
		self.to_string()
	}
}

impl Display for Graph {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		if self.what.0.len() <= 1 && self.cond.is_none() && self.alias.is_none() {
			Display::fmt(&self.dir, f)?;
			match self.what.len() {
				0 => f.write_char('?'),
				_ => Display::fmt(&self.what, f),
			}
		} else {
			write!(f, "{}(", self.dir)?;
			match self.what.len() {
				0 => f.write_char('?'),
				_ => Display::fmt(&self.what, f),
			}?;
			if let Some(ref v) = self.cond {
				write!(f, " {v}")?
			}
			if let Some(ref v) = self.split {
				write!(f, " {v}")?
			}
			if let Some(ref v) = self.group {
				write!(f, " {v}")?
			}
			if let Some(ref v) = self.order {
				write!(f, " {v}")?
			}
			if let Some(ref v) = self.limit {
				write!(f, " {v}")?
			}
			if let Some(ref v) = self.start {
				write!(f, " {v}")?
			}
			if let Some(ref v) = self.alias {
				write!(f, " AS {v}")?
			}
			f.write_char(')')
		}
	}
}
