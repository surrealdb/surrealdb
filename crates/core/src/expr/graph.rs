use std::fmt::{self, Display, Formatter, Write};
use std::ops::Bound;

use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::fmt::Fmt;
use crate::expr::order::Ordering;
use crate::expr::start::Start;
use crate::expr::{Cond, Dir, Fields, Groups, Ident, Idiom, Limit, RecordIdKeyRangeLit, Splits};
use crate::kvs::KVKey;
use crate::val::{RecordId, RecordIdKey, RecordIdKeyRange};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
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

impl Graph {
	/// Convert the graph edge to a raw String
	pub fn to_raw(&self) -> String {
		self.to_string()
	}
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
			match self.what.len() {
				0 => f.write_char('?'),
				_ => Fmt::comma_separated(self.what.iter()).fmt(f),
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum GraphSubject {
	Table(Ident),
	Range {
		table: Ident,
		range: RecordIdKeyRangeLit,
	},
}

impl GraphSubject {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<ComputedGraphSubject> {
		match self {
			GraphSubject::Table(ident) => Ok(ComputedGraphSubject::Table(ident.clone())),
			GraphSubject::Range {
				table,
				range,
			} => Ok(ComputedGraphSubject::Range {
				table: table.clone(),
				range: range.compute(stk, ctx, opt, doc).await?,
			}),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum ComputedGraphSubject {
	Table(Ident),
	Range {
		table: Ident,
		range: RecordIdKeyRange,
	},
}

impl ComputedGraphSubject {
	pub fn into_literal(self) -> GraphSubject {
		match self {
			ComputedGraphSubject::Table(ident) => GraphSubject::Table(ident),
			ComputedGraphSubject::Range {
				table,
				range,
			} => GraphSubject::Range {
				table,
				range: range.into_literal(),
			},
		}
	}

	pub(crate) fn presuf(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordIdKey,
		dir: &Dir,
	) -> (Result<Vec<u8>>, Result<Vec<u8>>) {
		match self {
			Self::Table(t) => (
				crate::key::graph::ftprefix(ns, db, tb, id, dir, t),
				crate::key::graph::ftsuffix(ns, db, tb, id, dir, t),
			),
			Self::Range {
				table,
				range,
			} => {
				let beg = match &range.start {
					Bound::Unbounded => crate::key::graph::ftprefix(ns, db, tb, id, dir, table),
					Bound::Included(v) => crate::key::graph::new(
						ns,
						db,
						tb,
						id,
						dir,
						&RecordId {
							table: table.clone().into_string(),
							key: v.clone(),
						},
					)
					.encode_key(),
					Bound::Excluded(v) => crate::key::graph::new(
						ns,
						db,
						tb,
						id,
						dir,
						&RecordId {
							table: table.clone().into_string(),
							key: v.to_owned(),
						},
					)
					.encode_key()
					.map(|mut v| {
						v.push(0x00);
						v
					}),
				};
				// Prepare the range end key
				let end = match &range.end {
					Bound::Unbounded => crate::key::graph::ftsuffix(ns, db, tb, id, dir, table),
					Bound::Excluded(v) => crate::key::graph::new(
						ns,
						db,
						tb,
						id,
						dir,
						&RecordId {
							table: table.clone().into_string(),
							key: v.to_owned(),
						},
					)
					.encode_key(),
					Bound::Included(v) => crate::key::graph::new(
						ns,
						db,
						tb,
						id,
						dir,
						&RecordId {
							table: table.clone().into_string(),
							key: v.to_owned(),
						},
					)
					.encode_key()
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
