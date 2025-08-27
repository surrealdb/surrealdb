use std::fmt::{self, Display, Formatter, Write};

use super::Ident;
use crate::sql::fmt::Fmt;
use crate::sql::order::Ordering;
use crate::sql::{Cond, Dir, Fields, Groups, Idiom, Limit, RecordIdKeyRangeLit, Splits, Start};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Graph {
	pub dir: Dir,
	pub expr: Option<Fields>,
	pub what: Vec<GraphSubject>,
	pub cond: Option<Cond>,
	pub split: Option<Splits>,
	pub group: Option<Groups>,
	pub order: Option<Ordering>,
	pub limit: Option<Limit>,
	pub start: Option<Start>,
	pub alias: Option<Idiom>,
}

impl Display for Graph {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		if self.what.len() <= 1
			&& self.cond.is_none()
			&& self.alias.is_none()
			&& self.expr.is_none()
		{
			Display::fmt(&self.dir, f)?;
			if self.what.is_empty() {
				f.write_char('?')
			} else {
				Fmt::comma_separated(self.what.iter()).fmt(f)
			}
		} else {
			write!(f, "{}(", self.dir)?;
			if let Some(ref expr) = self.expr {
				write!(f, "SELECT {} FROM ", expr)?;
			}
			if self.what.is_empty() {
				f.write_char('?')
			} else {
				Display::fmt(&Fmt::comma_separated(&self.what), f)
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

impl From<Graph> for crate::expr::Graph {
	fn from(v: Graph) -> Self {
		Self {
			dir: v.dir.into(),
			expr: v.expr.map(From::from),
			what: v.what.into_iter().map(From::from).collect(),
			cond: v.cond.map(Into::into),
			split: v.split.map(Into::into),
			group: v.group.map(Into::into),
			order: v.order.map(Into::into),
			limit: v.limit.map(Into::into),
			start: v.start.map(Into::into),
			alias: v.alias.map(Into::into),
		}
	}
}

impl From<crate::expr::Graph> for Graph {
	fn from(v: crate::expr::Graph) -> Self {
		Graph {
			dir: v.dir.into(),
			expr: v.expr.map(Into::into),
			what: v.what.into_iter().map(From::from).collect(),
			cond: v.cond.map(Into::into),
			split: v.split.map(Into::into),
			group: v.group.map(Into::into),
			order: v.order.map(Into::into),
			limit: v.limit.map(Into::into),
			start: v.start.map(Into::into),
			alias: v.alias.map(Into::into),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum GraphSubject {
	Table(Ident),
	Range(Ident, RecordIdKeyRangeLit),
}

impl Display for GraphSubject {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Table(tb) => Display::fmt(&tb, f),
			Self::Range(tb, rng) => write!(f, "{tb}:{rng}"),
		}
	}
}

impl From<GraphSubject> for crate::expr::graph::GraphSubject {
	fn from(v: GraphSubject) -> Self {
		match v {
			GraphSubject::Table(tb) => Self::Table(tb.into()),
			GraphSubject::Range(table, range) => Self::Range {
				table: table.into(),
				range: range.into(),
			},
		}
	}
}

impl From<crate::expr::graph::GraphSubject> for GraphSubject {
	fn from(v: crate::expr::graph::GraphSubject) -> Self {
		match v {
			crate::expr::graph::GraphSubject::Table(tb) => Self::Table(tb.into()),
			crate::expr::graph::GraphSubject::Range {
				table,
				range,
			} => Self::Range(table.into(), range.into()),
		}
	}
}
