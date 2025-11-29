use std::fmt::{self, Display, Formatter, Write};
use std::ops::Bound;

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::order::Ordering;
use crate::expr::start::Start;
use crate::expr::{Cond, Dir, Fields, Groups, Idiom, Limit, RecordIdKeyRangeLit, Splits};
use crate::fmt::{EscapeIdent, EscapeKwFreeIdent, Fmt};
use crate::kvs::KVKey;
use crate::val::{RecordId, RecordIdKey, RecordIdKeyRange};

/// A lookup is a unified way of looking up graph edges and record references.
/// Since they both work very similarly, they also both support the same operations
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct Lookup {
	pub(crate) kind: LookupKind,
	pub(crate) expr: Option<Fields>,
	pub(crate) what: Vec<LookupSubject>,
	pub(crate) cond: Option<Cond>,
	pub(crate) split: Option<Splits>,
	pub(crate) group: Option<Groups>,
	pub(crate) order: Option<Ordering>,
	pub(crate) limit: Option<Limit>,
	pub(crate) start: Option<Start>,
	pub(crate) alias: Option<Idiom>,
}

impl Lookup {
	/// Convert the graph edge to a raw String
	pub fn to_raw(&self) -> String {
		self.to_string()
	}
}

impl Display for Lookup {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		if self.what.len() <= 1
			// When the singular lookup subject has a referencing field, it needs to be wrapped in parentheses
			// Otherwise <~table.field will be parsed as [Lookup(<~table), Field(.field)]
			// Whereas <~(table.field) will be parsed as [Lookup(<~table.field)]
			//
			//
			// Further more `<-foo:a..` can lead to issues when the next part of the idiom starts
			// with a `.`
			&& self.what.iter().all(|v| {
				if v.referencing_field().is_some() {
					return false
				}
				if let LookupSubject::Range { range: RecordIdKeyRangeLit{ end: Bound::Unbounded, .. }, ..} = v {
					return false
				}
				true
			})
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

/// This enum instructs whether the lookup is a graph edge or a record reference
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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

/// This enum instructs whether we scan all edges on a table or just a specific range
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum LookupSubject {
	Table {
		table: String,
		referencing_field: Option<String>,
	},
	Range {
		table: String,
		range: RecordIdKeyRangeLit,
		referencing_field: Option<String>,
	},
}

impl LookupSubject {
	pub(crate) fn referencing_field(&self) -> Option<&String> {
		match self {
			LookupSubject::Table {
				referencing_field,
				..
			} => referencing_field.as_ref(),
			LookupSubject::Range {
				referencing_field,
				..
			} => referencing_field.as_ref(),
		}
	}
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
			LookupSubject::Table {
				table,
				referencing_field,
			} => Ok(ComputedLookupSubject::Table {
				table: table.clone(),
				referencing_field: referencing_field.clone(),
			}),
			LookupSubject::Range {
				table,
				range,
				referencing_field,
			} => Ok(ComputedLookupSubject::Range {
				table: table.clone(),
				range: range.compute(stk, ctx, opt, doc).await?,
				referencing_field: referencing_field.clone(),
			}),
		}
	}
}

/// This enum instructs whether we scan all edges on a table or just a specific range
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ComputedLookupSubject {
	Table {
		table: String,
		referencing_field: Option<String>,
	},
	Range {
		table: String,
		range: RecordIdKeyRange,
		referencing_field: Option<String>,
	},
}

impl ComputedLookupSubject {
	pub fn into_literal(self) -> LookupSubject {
		match self {
			ComputedLookupSubject::Table {
				table,
				referencing_field,
			} => LookupSubject::Table {
				table: table.clone(),
				referencing_field: referencing_field.clone(),
			},
			ComputedLookupSubject::Range {
				table,
				range,
				referencing_field,
			} => LookupSubject::Range {
				table,
				range: range.into_literal(),
				referencing_field: referencing_field.clone(),
			},
		}
	}

	/// The presuf function generates the prefix and suffix keys for a lookup
	/// based on the lookup subject and the lookup kind
	pub(crate) fn presuf(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &str,
		id: &RecordIdKey,
		kind: &LookupKind,
	) -> Result<(Vec<u8>, Vec<u8>)> {
		match kind {
			// We're looking up record references
			LookupKind::Reference => match self {
				// Scan the entire range
				Self::Table {
					table,
					referencing_field: None,
				} => Ok((
					crate::key::r#ref::ftprefix(ns, db, tb, id, table)?,
					crate::key::r#ref::ftsuffix(ns, db, tb, id, table)?,
				)),
				// Scan the entire range with a referencing field
				Self::Table {
					table,
					referencing_field: Some(field),
				} => Ok((
					crate::key::r#ref::ffprefix(ns, db, tb, id, table, field)?,
					crate::key::r#ref::ffsuffix(ns, db, tb, id, table, field)?,
				)),
				// Scan a specific range
				Self::Range {
					table,
					range,
					referencing_field,
				} => {
					let Some(field) = referencing_field else {
						bail!(
							"Cannot scan a specific range of record references without a referencing field"
						);
					};
					let beg = match &range.start {
						Bound::Unbounded => {
							crate::key::r#ref::ffprefix(ns, db, tb, id, table, field)?
						}
						Bound::Included(v) => {
							crate::key::r#ref::refprefix(ns, db, tb, id, table, field, v)?
						}
						Bound::Excluded(v) => {
							crate::key::r#ref::refsuffix(ns, db, tb, id, table, field, v)?
						}
					};
					// Prepare the range end key
					let end = match &range.end {
						Bound::Unbounded => {
							crate::key::r#ref::ffsuffix(ns, db, tb, id, table, field)?
						}
						Bound::Excluded(v) => {
							crate::key::r#ref::refprefix(ns, db, tb, id, table, field, v)?
						}
						Bound::Included(v) => {
							crate::key::r#ref::refsuffix(ns, db, tb, id, table, field, v)?
						}
					};

					Ok((beg, end))
				}
			},
			// We're looking up graph edges
			LookupKind::Graph(dir) => match self {
				// Scan the entire range
				Self::Table {
					table,
					..
				} => Ok((
					crate::key::graph::ftprefix(ns, db, tb, id, dir, table)?,
					crate::key::graph::ftsuffix(ns, db, tb, id, dir, table)?,
				)),
				// Scan a specific range
				Self::Range {
					table,
					range,
					..
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
								table: table.clone(),
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
								table: table.clone(),
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
								table: table.clone(),
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
								table: table.clone(),
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
			Self::Table {
				table,
				referencing_field,
			} => {
				EscapeIdent(table).fmt(f)?;
				if let Some(referencing_field) = referencing_field {
					write!(f, " FIELD {}", EscapeKwFreeIdent(referencing_field))?;
				}
				Ok(())
			}
			Self::Range {
				table,
				range,
				referencing_field,
			} => {
				write!(f, "{}:{range}", EscapeKwFreeIdent(table))?;
				if let Some(referencing_field) = referencing_field {
					write!(f, " FIELD {}", EscapeKwFreeIdent(referencing_field))?;
				}
				Ok(())
			}
		}
	}
}
