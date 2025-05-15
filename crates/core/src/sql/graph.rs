use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::exe::try_join_all_buffered;
use crate::kvs::KeyEncode;
use crate::sql::cond::Cond;
use crate::sql::dir::Dir;
use crate::sql::field::Fields;
use crate::sql::group::Groups;
use crate::sql::idiom::Idiom;
use crate::sql::limit::Limit;
use crate::sql::order::{OldOrders, Order, OrderList, Ordering};
use crate::sql::split::Splits;
use crate::sql::start::Start;
use crate::sql::table::Tables;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter, Write};
use std::ops::{Bound, Deref};

use super::fmt::Fmt;
use super::{Id, IdRange, Table, Thing};

#[revisioned(revision = 4)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Graph {
	pub dir: Dir,
	#[revision(end = 3, convert_fn = "convert_old_expr")]
	pub old_expr: Fields,
	#[revision(start = 3)]
	pub expr: Option<Fields>,
	#[revision(end = 4, convert_fn = "convert_old_what")]
	pub _what: Tables,
	#[revision(start = 4)]
	pub what: GraphSubjects,
	pub cond: Option<Cond>,
	pub split: Option<Splits>,
	pub group: Option<Groups>,
	#[revision(end = 2, convert_fn = "convert_old_orders")]
	pub old_order: Option<OldOrders>,
	#[revision(start = 2)]
	pub order: Option<Ordering>,
	pub limit: Option<Limit>,
	pub start: Option<Start>,
	pub alias: Option<Idiom>,
}

impl Graph {
	fn convert_old_orders(
		&mut self,
		_rev: u16,
		old_value: Option<OldOrders>,
	) -> Result<(), revision::Error> {
		let Some(x) = old_value else {
			// nothing to do.
			return Ok(());
		};

		if x.0.iter().any(|x| x.random) {
			self.order = Some(Ordering::Random);
			return Ok(());
		}

		let new_ord =
			x.0.into_iter()
				.map(|x| Order {
					value: x.order,
					collate: x.collate,
					numeric: x.numeric,
					direction: x.direction,
				})
				.collect();

		self.order = Some(Ordering::Order(OrderList(new_ord)));

		Ok(())
	}

	fn convert_old_what(&mut self, _rev: u16, old: Tables) -> Result<(), revision::Error> {
		self.what = old.into();
		Ok(())
	}

	fn convert_old_expr(&mut self, _rev: u16, _old_value: Fields) -> Result<(), revision::Error> {
		// Before this change, users would not have been able to set the value of the `expr` field, it's always `Fields(vec![Field::All], false)`.
		// None is the new default value, mimmicking that behaviour.
		self.expr = None;
		Ok(())
	}

	/// Convert the graph edge to a raw String
	pub fn to_raw(&self) -> String {
		self.to_string()
	}
}

crate::sql::impl_display_from_sql!(Graph);

impl crate::sql::DisplaySql for Graph {
	fn fmt_sql(&self, f: &mut Formatter) -> fmt::Result {
		if self.what.0.len() <= 1
			&& self.cond.is_none()
			&& self.alias.is_none()
			&& self.expr.is_none()
		{
			Display::fmt(&self.dir, f)?;
			match self.what.len() {
				0 => f.write_char('?'),
				_ => Display::fmt(&self.what, f),
			}
		} else {
			write!(f, "{}(", self.dir)?;
			if let Some(ref expr) = self.expr {
				write!(f, "SELECT {} FROM ", expr)?;
			}
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
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct GraphSubjects(pub Vec<GraphSubject>);

impl From<Tables> for GraphSubjects {
	fn from(tbs: Tables) -> Self {
		Self(tbs.0.into_iter().map(GraphSubject::from).collect())
	}
}

impl From<Table> for GraphSubjects {
	fn from(v: Table) -> Self {
		GraphSubjects(vec![GraphSubject::Table(v)])
	}
}

impl Deref for GraphSubjects {
	type Target = Vec<GraphSubject>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

crate::sql::impl_display_from_sql!(GraphSubjects);

impl crate::sql::DisplaySql for GraphSubjects {
	fn fmt_sql(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&Fmt::comma_separated(&self.0), f)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum GraphSubject {
	Table(Table),
	Range(Table, IdRange),
}

impl From<Table> for GraphSubject {
	fn from(x: Table) -> Self {
		GraphSubject::Table(x)
	}
}

crate::sql::impl_display_from_sql!(GraphSubject);

impl crate::sql::DisplaySql for GraphSubject {
	fn fmt_sql(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Table(tb) => Display::fmt(&tb, f),
			Self::Range(tb, rng) => write!(f, "{tb}:{rng}"),
		}
	}
}
