use crate::cond::Cond;
use crate::dir::Dir;
use crate::field::Fields;
use crate::group::Groups;
use crate::idiom::Idiom;
use crate::limit::Limit;
use crate::order::Orders;
use crate::split::Splits;
use crate::start::Start;
use crate::table::Tables;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter, Write};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
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
