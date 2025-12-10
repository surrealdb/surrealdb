use std::ops::Bound;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::order::Ordering;
use crate::expr::start::Start;
use crate::expr::{Cond, Dir, Fields, Groups, Idiom, Limit, RecordIdKeyRangeLit, Splits};
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

impl ToSql for Lookup {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::lookup::Lookup = self.clone().into();
		stmt.fmt_sql(f, fmt);
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

impl ToSql for LookupKind {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Graph(dir) => dir.fmt_sql(f, fmt),
			Self::Reference => f.push_str("<~"),
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
	#[instrument(level = "trace", name = "LookupSubject::compute", skip_all)]
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

impl ToSql for LookupSubject {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::lookup::LookupSubject = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
