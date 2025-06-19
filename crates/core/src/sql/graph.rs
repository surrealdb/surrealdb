use crate::sql::order::Ordering;
use crate::sql::{
	Cond, Dir, Fields, Groups, IdRange, Idiom, Limit, Splits, Start, Table, fmt::Fmt,
};
use std::fmt::{self, Display, Formatter, Write};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
pub struct Graph {
	pub dir: Dir,
	pub expr: Option<Fields>,
	pub what: GraphSubjects,
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
		if self.what.0.len() <= 1
			&& self.cond.is_none()
			&& self.alias.is_none()
			&& self.expr.is_none()
		{
			Display::fmt(&self.dir, f)?;
			match self.what.0.len() {
				0 => f.write_char('?'),
				_ => Display::fmt(&self.what, f),
			}
		} else {
			write!(f, "{}(", self.dir)?;
			if let Some(ref expr) = self.expr {
				write!(f, "SELECT {} FROM ", expr)?;
			}
			match self.what.0.len() {
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

impl From<Graph> for crate::expr::Graph {
	fn from(v: Graph) -> Self {
		Self {
			dir: v.dir.into(),
			expr: v.expr.map(Into::into),
			what: v.what.into(),
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
			what: v.what.into(),
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

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct GraphSubjects(pub Vec<GraphSubject>);

impl From<GraphSubjects> for crate::expr::graph::GraphSubjects {
	fn from(v: GraphSubjects) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}
impl From<crate::expr::graph::GraphSubjects> for GraphSubjects {
	fn from(v: crate::expr::graph::GraphSubjects) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

impl Display for GraphSubjects {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&Fmt::comma_separated(&self.0), f)
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum GraphSubject {
	Table(Table),
	Range(Table, IdRange),
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
			GraphSubject::Range(tb, rng) => Self::Range(tb.into(), rng.into()),
		}
	}
}

impl From<crate::expr::graph::GraphSubject> for GraphSubject {
	fn from(v: crate::expr::graph::GraphSubject) -> Self {
		match v {
			crate::expr::graph::GraphSubject::Table(tb) => Self::Table(tb.into()),
			crate::expr::graph::GraphSubject::Range(tb, rng) => Self::Range(tb.into(), rng.into()),
		}
	}
}
