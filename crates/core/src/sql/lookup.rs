use std::fmt::{self, Display, Formatter, Write};

use super::Ident;
use crate::sql::fmt::Fmt;
use crate::sql::order::Ordering;
use crate::sql::{Cond, Dir, Fields, Groups, Idiom, Limit, RecordIdKeyRangeLit, Splits, Start};

/// A lookup is a unified way of looking up graph edges and record references.
/// Since they both work very similarly, they also both support the same operations
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Lookup {
	pub kind: LookupKind,
	pub expr: Option<Fields>,
	pub what: Vec<LookupSubject>,
	pub cond: Option<Cond>,
	pub split: Option<Splits>,
	pub group: Option<Groups>,
	pub order: Option<Ordering>,
	pub limit: Option<Limit>,
	pub start: Option<Start>,
	pub alias: Option<Idiom>,
}

impl Display for Lookup {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		if self.what.len() <= 1
			&& self.cond.is_none()
			&& self.alias.is_none()
			&& self.expr.is_none()
		{
			Display::fmt(&self.kind, f)?;
			if self.what.is_empty() {
				f.write_char('?')
			} else {
				Fmt::comma_separated(self.what.iter()).fmt(f)
			}
		} else {
			write!(f, "{}(", self.kind)?;
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

impl From<Lookup> for crate::expr::Lookup {
	fn from(v: Lookup) -> Self {
		Self {
			kind: v.kind.into(),
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

impl From<crate::expr::Lookup> for Lookup {
	fn from(v: crate::expr::Lookup) -> Self {
		Lookup {
			kind: v.kind.into(),
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

/// This enum instructs whether the lookup is a graph edge or a record reference
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum LookupKind {
	Graph(Dir),
	Reference,
}

impl LookupKind {
	pub fn is_graph(&self) -> bool {
		matches!(self, LookupKind::Graph(_))
	}

	pub fn is_reference(&self) -> bool {
		matches!(self, LookupKind::Reference)
	}
}

impl Default for LookupKind {
	fn default() -> Self {
		Self::Graph(Dir::Both)
	}
}

impl Display for LookupKind {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Graph(dir) => Display::fmt(dir, f),
			Self::Reference => write!(f, "<~"),
		}
	}
}

impl From<LookupKind> for crate::expr::lookup::LookupKind {
	fn from(v: LookupKind) -> Self {
		match v {
			LookupKind::Graph(dir) => Self::Graph(dir.into()),
			LookupKind::Reference => Self::Reference,
		}
	}
}

impl From<crate::expr::lookup::LookupKind> for LookupKind {
	fn from(v: crate::expr::lookup::LookupKind) -> Self {
		match v {
			crate::expr::lookup::LookupKind::Graph(dir) => Self::Graph(dir.into()),
			crate::expr::lookup::LookupKind::Reference => Self::Reference,
		}
	}
}

/// This enum instructs whether we scan all edges on a table or just a specific range
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum LookupSubject {
	Table(Ident),
	Range(Ident, RecordIdKeyRangeLit),
}

impl Display for LookupSubject {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Table(tb) => Display::fmt(&tb, f),
			Self::Range(tb, rng) => write!(f, "{tb}:{rng}"),
		}
	}
}

impl From<LookupSubject> for crate::expr::lookup::LookupSubject {
	fn from(v: LookupSubject) -> Self {
		match v {
			LookupSubject::Table(tb) => Self::Table(tb.into()),
			LookupSubject::Range(table, range) => Self::Range {
				table: table.into(),
				range: range.into(),
			},
		}
	}
}

impl From<crate::expr::lookup::LookupSubject> for LookupSubject {
	fn from(v: crate::expr::lookup::LookupSubject) -> Self {
		match v {
			crate::expr::lookup::LookupSubject::Table(tb) => Self::Table(tb.into()),
			crate::expr::lookup::LookupSubject::Range {
				table,
				range,
			} => Self::Range(table.into(), range.into()),
		}
	}
}
