use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::try_join_all_buffered;
use crate::expr::order::Ordering;
use crate::expr::start::Start;
use crate::expr::table::Tables;
use crate::expr::{Cond, Dir, Fields, Groups, Idiom, Limit, RecordIdKeyLit, Splits, Table};
use crate::iam::ResourceKind;
use crate::kvs::KeyEncode;
use crate::val::{RecordId, RecordIdKey, RecordIdKeyRange};
use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter, Write};
use std::ops::{Bound, Deref};

use super::RecordIdKeyRangeLit;
use super::fmt::Fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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

impl Graph {
	/// Convert the graph edge to a raw String
	pub fn to_raw(&self) -> String {
		self.to_string()
	}
}

impl Display for Graph {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
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

/*
impl From<Table> for GraphSubjects {
	fn from(v: Table) -> Self {
		GraphSubjects(vec![GraphSubject::Table(v)])
	}
}
*/

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
	) -> Result<Self> {
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum GraphSubject {
	Table(Table),
	Range {
		table: Table,
		range: RecordIdKeyRange,
	},
}

impl GraphSubject {
	pub(crate) async fn compute(
		self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Self> {
		if let Self::Range {
			table,
			range,
		} = self
		{
			let range = range.compute(stk, ctx, opt, doc).await?;
			Ok(Self::Range {
				table,
				range,
			})
		} else {
			Ok(self)
		}
	}

	pub(crate) fn presuf(
		&self,
		ns: &str,
		db: &str,
		tb: &str,
		id: &RecordIdKey,
		dir: &Dir,
	) -> (Result<Vec<u8>>, Result<Vec<u8>>) {
		match self {
			Self::Table(t) => (
				crate::key::graph::ftprefix(ns, db, tb, id, dir, &t.0),
				crate::key::graph::ftsuffix(ns, db, tb, id, dir, &t.0),
			),
			Self::Range {
				table,
				range,
			} => {
				let beg = match &range.start {
					Bound::Unbounded => crate::key::graph::ftprefix(ns, db, tb, id, dir, &table.0),
					Bound::Included(v) => crate::key::graph::new(
						ns,
						db,
						tb,
						id,
						dir,
						&RecordId {
							table: table.0.clone(),
							key: v.to_owned(),
						},
					)
					.encode(),
					Bound::Excluded(v) => crate::key::graph::new(
						ns,
						db,
						tb,
						id,
						dir,
						&RecordId {
							table: table.0.clone(),
							key: v.to_owned(),
						},
					)
					.encode()
					.map(|mut v| {
						v.push(0x00);
						v
					}),
				};
				// Prepare the range end key
				let end = match &range.end {
					Bound::Unbounded => crate::key::graph::ftsuffix(ns, db, tb, id, dir, &table.0),
					Bound::Excluded(v) => crate::key::graph::new(
						ns,
						db,
						tb,
						id,
						dir,
						&RecordId {
							table: table.0.clone(),
							key: v.to_owned(),
						},
					)
					.encode(),
					Bound::Included(v) => crate::key::graph::new(
						ns,
						db,
						tb,
						id,
						dir,
						&RecordId {
							table: table.0.clone(),
							key: v.to_owned(),
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
			Self::Range {
				table,
				range,
			} => write!(f, "{table}:{range}"),
		}
	}
}
