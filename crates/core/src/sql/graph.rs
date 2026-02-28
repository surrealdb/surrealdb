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

impl Display for Graph {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		if self.what.0.is_empty() {
			write!(f, "{}?", self.dir)
		} else if self.what.0.len() == 1
			&& self.cond.is_none()
			&& self.alias.is_none()
			&& self.split.is_none()
			&& self.order.is_none()
			&& self.limit.is_none()
			&& self.start.is_none()
			&& self.expr.is_none()
		{
			write!(f, "{}{}", self.dir, self.what)
		} else {
			write!(f, "{}(", self.dir)?;
			if let Some(x) = &self.expr {
				f.write_str("SELECT ")?;
				write!(f, "{x}")?;
				f.write_str(" FROM ")?;
			};

			Display::fmt(&self.what, f)?;

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
pub struct GraphSubjects(
	#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::sql::arbitrary::atleast_one))]
	pub  Vec<GraphSubject>,
);

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

impl GraphSubjects {
	pub(crate) async fn compute(
		self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Self, Error> {
		stk.scope(|scope| {
			let futs = self.0.into_iter().map(|v| scope.run(|stk| v.compute(stk, ctx, opt, doc)));
			try_join_all_buffered(futs)
		})
		.await
		.map(GraphSubjects)
	}
}

impl Display for GraphSubjects {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
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

impl GraphSubject {
	pub(crate) async fn compute(
		self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Self, Error> {
		if let Self::Range(tb, rng) = self {
			let rng = rng.compute(stk, ctx, opt, doc).await?;
			Ok(Self::Range(tb, rng))
		} else {
			Ok(self)
		}
	}

	pub(crate) fn presuf(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		id: &Id,
		dir: &Dir,
	) -> (Result<Vec<u8>, Error>, Result<Vec<u8>, Error>) {
		match self {
			Self::Table(t) => (
				crate::key::graph::ftprefix(ns, db, tb, id, dir, &t.0),
				crate::key::graph::ftsuffix(ns, db, tb, id, dir, &t.0),
			),
			Self::Range(t, r) => {
				let beg = match &r.beg {
					Bound::Unbounded => crate::key::graph::ftprefix(ns, db, tb, id, dir, &t.0),
					Bound::Included(v) => crate::key::graph::new(
						ns,
						db,
						tb,
						id,
						dir,
						&Thing {
							tb: t.0.clone(),
							id: v.to_owned(),
						},
					)
					.encode(),
					Bound::Excluded(v) => crate::key::graph::new(
						ns,
						db,
						tb,
						id,
						dir,
						&Thing {
							tb: t.0.clone(),
							id: v.to_owned(),
						},
					)
					.encode()
					.map(|mut v| {
						v.push(0x00);
						v
					}),
				};
				// Prepare the range end key
				let end = match &r.end {
					Bound::Unbounded => crate::key::graph::ftsuffix(ns, db, tb, id, dir, &t.0),
					Bound::Excluded(v) => crate::key::graph::new(
						ns,
						db,
						tb,
						id,
						dir,
						&Thing {
							tb: t.0.clone(),
							id: v.to_owned(),
						},
					)
					.encode(),
					Bound::Included(v) => crate::key::graph::new(
						ns,
						db,
						tb,
						id,
						dir,
						&Thing {
							tb: t.0.clone(),
							id: v.to_owned(),
						},
					)
					.encode()
					.map(|mut v| {
						v.push(0x00);
						v
					}),
				};

				(beg, end)
			}
		}
	}
}

impl From<Table> for GraphSubject {
	fn from(x: Table) -> Self {
		GraphSubject::Table(x)
	}
}

impl Display for GraphSubject {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Table(tb) => Display::fmt(&tb, f),
			Self::Range(tb, rng) => write!(f, "{tb}:{rng}"),
		}
	}
}
