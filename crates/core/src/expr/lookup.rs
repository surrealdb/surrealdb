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

impl Lookup {
	/// Convert the graph edge to a raw String
	pub fn to_raw(&self) -> String {
		self.to_string()
	}
}

impl Display for Lookup {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		let what_contained = self.what.len() > 1
			|| self.what.iter().any(|w| matches!(w, LookupSubject::Field(_, _)));
		if !what_contained && self.cond.is_none() && self.alias.is_none() && self.expr.is_none() {
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
pub enum LookupKind {
	Graph(Dir),
	Reference,
}

impl Default for LookupKind {
	fn default() -> Self {
		Self::Graph(Dir::default())
	}
}

impl Display for LookupKind {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Graph(dir) => Display::fmt(dir, f),
			Self::Reference => f.write_str("<~"),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum LookupSubject {
	Table(Ident),
	Field(Ident, Ident),
	Range {
		table: Ident,
		range: RecordIdKeyRangeLit,
	},
}

impl LookupSubject {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<ComputedLookupSubject> {
		match self {
			LookupSubject::Table(ident) => Ok(ComputedLookupSubject::Table(ident.clone())),
			LookupSubject::Field(ident, field) => {
				Ok(ComputedLookupSubject::Field(ident.clone(), field.clone()))
			}
			LookupSubject::Range {
				table,
				range,
			} => Ok(ComputedLookupSubject::Range {
				table: table.clone(),
				range: range.compute(stk, ctx, opt, doc).await?,
			}),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum ComputedLookupSubject {
	Table(Ident),
	Field(Ident, Ident),
	Range {
		table: Ident,
		range: RecordIdKeyRange,
	},
}

impl ComputedLookupSubject {
	pub fn into_literal(self) -> LookupSubject {
		match self {
			ComputedLookupSubject::Table(ident) => LookupSubject::Table(ident),
			ComputedLookupSubject::Field(ident, field) => LookupSubject::Field(ident, field),
			ComputedLookupSubject::Range {
				table,
				range,
			} => LookupSubject::Range {
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
		kind: &LookupKind,
	) -> Result<(Vec<u8>, Vec<u8>)> {
		match kind {
			LookupKind::Reference => match self {
				Self::Table(t) => Ok((
					crate::key::r#ref::ftprefix(ns, db, tb, id, t)?,
					crate::key::r#ref::ftsuffix(ns, db, tb, id, t)?,
				)),
				Self::Field(ft, ff) => Ok((
					crate::key::r#ref::ffprefix(ns, db, tb, id, ft, ff)?,
					crate::key::r#ref::ffsuffix(ns, db, tb, id, ft, ff)?,
				)),
				Self::Range {
					..
				} => {
					// Based on the parser it is not possible for a user to get here
					fail!("Range lookup not supported for reference")
				}
			},
			LookupKind::Graph(dir) => match self {
				Self::Table(t) => Ok((
					crate::key::graph::ftprefix(ns, db, tb, id, dir, t)?,
					crate::key::graph::ftsuffix(ns, db, tb, id, dir, t)?,
				)),
				Self::Field(_, _) => {
					// Based on the parser it is not possible for a user to get here
					fail!("Field lookup not supported for graph")
				}
				Self::Range {
					table,
					range,
				} => {
					let beg = match &range.start {
						Bound::Unbounded => {
							crate::key::graph::ftprefix(ns, db, tb, id, dir, table)?
						}
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
						.encode_key()?,
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
						})?,
					};
					// Prepare the range end key
					let end = match &range.end {
						Bound::Unbounded => {
							crate::key::graph::ftsuffix(ns, db, tb, id, dir, table)?
						}
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
						.encode_key()?,
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
						})?,
					};

					Ok((beg, end))
				}
			},
		}
	}
}

impl Display for LookupSubject {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Table(tb) => Display::fmt(&tb, f),
			Self::Field(tb, field) => write!(f, "{tb}.{field}"),
			Self::Range {
				table,
				range,
			} => write!(f, "{table}:{range}"),
		}
	}
}
