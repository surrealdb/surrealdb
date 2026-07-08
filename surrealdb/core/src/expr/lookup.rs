use std::borrow::Cow;
use std::ops::Bound;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::order::Ordering;
use crate::expr::start::Start;
use crate::expr::{Cond, Dir, Fields, Groups, Idiom, Limit, RecordIdKeyRangeLit, Splits};
use crate::key::database::all::DatabaseRoot;
use crate::key::{KVKey, KVRange, KeyRange};
use crate::val::{RecordIdKey, RecordIdKeyRange, TableName};

/// A lookup is a unified way of looking up graph edges and record references.
/// Since they both work very similarly, they also both support the same operations
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct Lookup {
	pub(crate) kind: LookupKind,
	pub(crate) expr: Option<Fields>,
	pub(crate) only: bool,
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
		table: TableName,
		referencing_field: Option<String>,
	},
	Range {
		table: TableName,
		range: RecordIdKeyRangeLit,
		referencing_field: Option<String>,
	},
}

impl LookupSubject {
	#[instrument(level = "trace", name = "LookupSubject::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
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
		table: TableName,
		referencing_field: Option<String>,
	},
	Range {
		table: TableName,
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
				table,
				referencing_field,
			},
			ComputedLookupSubject::Range {
				table,
				range,
				referencing_field,
			} => LookupSubject::Range {
				table,
				range: range.into_literal(),
				referencing_field,
			},
		}
	}

	/// The presuf function generates the prefix and suffix keys for a lookup
	/// based on the lookup subject and the lookup kind
	pub(crate) fn presuf(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordIdKey,
		kind: &LookupKind,
	) -> Result<KeyRange<'_>> {
		let prefix = DatabaseRoot {
			ns,
			db,
		};
		match kind {
			// We're looking up record references
			LookupKind::Reference => match self {
				// Scan the entire range
				Self::Table {
					table,
					referencing_field: None,
				} => crate::key::r#ref::PrefixFt {
					prefix,
					tb: Cow::Borrowed(tb),
					id: Cow::Borrowed(id),
					ft: Cow::Borrowed(table),
				}
				.encode_bound()
				.map(|x| x.prefix_expect()),
				// Scan the entire range with a referencing field
				Self::Table {
					table,
					referencing_field: Some(field),
				} => crate::key::r#ref::PrefixField {
					prefix,
					tb: Cow::Borrowed(tb),
					id: Cow::Borrowed(id),
					ft: Cow::Borrowed(table),
					ff: Cow::Borrowed(field),
				}
				.encode_bound()
				.map(|x| x.prefix_expect()),
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

					let start = match &range.start {
						Bound::Unbounded => crate::key::r#ref::PrefixField {
							prefix,
							tb: Cow::Borrowed(tb),
							id: Cow::Borrowed(id),
							ft: Cow::Borrowed(table),
							ff: Cow::Borrowed(field),
						}
						.encode_bound()?,
						Bound::Included(v) => crate::key::r#ref::Ref {
							prefix,
							table: Cow::Borrowed(tb),
							id: Cow::Borrowed(id),
							foreign_table: Cow::Borrowed(table),
							foreign_field: Cow::Borrowed(field),
							foreign_key: Cow::Borrowed(v),
						}
						.encode_key()?,
						Bound::Excluded(v) => crate::key::r#ref::Ref {
							prefix,
							table: Cow::Borrowed(tb),
							id: Cow::Borrowed(id),
							foreign_table: Cow::Borrowed(table),
							foreign_field: Cow::Borrowed(field),
							foreign_key: Cow::Borrowed(v),
						}
						.encode_key()?
						.next(),
					};
					// Prepare the range end key
					let end = match &range.end {
						Bound::Unbounded => crate::key::r#ref::PrefixField {
							prefix,
							tb: Cow::Borrowed(tb),
							id: Cow::Borrowed(id),
							ft: Cow::Borrowed(table),
							ff: Cow::Borrowed(field),
						}
						.encode_bound()?
						.next_neighbour()
						.expect("Reference prefix to have a neighbour"),
						Bound::Included(v) => crate::key::r#ref::Ref {
							prefix,
							table: Cow::Borrowed(tb),
							id: Cow::Borrowed(id),
							foreign_table: Cow::Borrowed(table),
							foreign_field: Cow::Borrowed(field),
							foreign_key: Cow::Borrowed(v),
						}
						.encode_key()?
						.next(),
						Bound::Excluded(v) => crate::key::r#ref::Ref {
							prefix,
							table: Cow::Borrowed(tb),
							id: Cow::Borrowed(id),
							foreign_table: Cow::Borrowed(table),
							foreign_field: Cow::Borrowed(field),
							foreign_key: Cow::Borrowed(v),
						}
						.encode_key()?,
					};

					Ok(KeyRange {
						start,
						end,
					})
				}
			},
			// We're looking up graph edges
			LookupKind::Graph(dir) => match self {
				// Scan the entire range
				Self::Table {
					table,
					..
				} => Ok(crate::key::graph::PrefixFt {
					prefix,
					tb: Cow::Borrowed(tb),
					id: Cow::Borrowed(id),
					dir: *dir,
					foreign_table: Cow::Borrowed(table),
				}
				.encode_range()?),
				// Scan a specific range
				Self::Range {
					table,
					range,
					..
				} => {
					let start = match &range.start {
						Bound::Unbounded => crate::key::graph::PrefixFt {
							prefix,
							tb: Cow::Borrowed(tb),
							id: Cow::Borrowed(id),
							dir: *dir,
							foreign_table: Cow::Borrowed(table),
						}
						.encode_bound()?,
						Bound::Included(v) => crate::key::graph::Graph {
							prefix,
							tb: Cow::Borrowed(tb),
							id: Cow::Borrowed(id),
							dir: *dir,
							foreign_table: Cow::Borrowed(table),
							foreign_key: Cow::Borrowed(v),
						}
						.encode_key()?,
						Bound::Excluded(v) => crate::key::graph::Graph {
							prefix,
							tb: Cow::Borrowed(tb),
							id: Cow::Borrowed(id),
							dir: *dir,
							foreign_table: Cow::Borrowed(table),
							foreign_key: Cow::Borrowed(v),
						}
						.encode_key()?
						// We need next_neighbour because this key is only a prefix of the actual
						// key stored in the KV store, next_neighbour will skip over any key which
						// has this key as a prefix.
						.next_neighbour_expect(),
					};
					// Prepare the range end key
					let end = match &range.end {
						Bound::Unbounded => crate::key::graph::PrefixFt {
							prefix,
							tb: Cow::Borrowed(tb),
							id: Cow::Borrowed(id),
							dir: *dir,
							foreign_table: Cow::Borrowed(table),
						}
						.encode_bound()?
						.next_neighbour()
						.expect("Expect the graph prefix to have a neighbour"),
						Bound::Included(v) => crate::key::graph::Graph {
							prefix,
							tb: Cow::Borrowed(tb),
							id: Cow::Borrowed(id),
							dir: *dir,
							foreign_table: Cow::Borrowed(table),
							foreign_key: Cow::Borrowed(v),
						}
						.encode_key()?
						// We need next_neighbour because this key is only a prefix of the actual
						// key stored in the KV store, next_neighbour will skip over any key which
						// has this key as a prefix.
						.next_neighbour_expect(),
						// Append `0xff` to include any new-format key for
						// this fk (target bytes follow the legacy encoding).
						Bound::Excluded(v) => crate::key::graph::Graph {
							prefix,
							tb: Cow::Borrowed(tb),
							id: Cow::Borrowed(id),
							dir: *dir,
							foreign_table: Cow::Borrowed(table),
							foreign_key: Cow::Borrowed(v),
						}
						.encode_key()?,
					};

					Ok(KeyRange {
						start,
						end,
					})
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
