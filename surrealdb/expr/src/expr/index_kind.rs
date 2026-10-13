//! The parsed index-parameter clause of a DEFINE INDEX statement.
//!
//! Mirrors the catalog's `Index` with the COUNT condition held as a parsed
//! expression rather than canonical text; lowering to a definition renders it
//! through the catalog's stored-text funnel.

use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::Cond;
use crate::sql;
use crate::sql::index::{DiskAnnParams, FullTextParams, HnswParams};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum Index {
	/// (Basic) non unique
	#[default]
	Idx,
	/// Unique index
	Uniq,
	/// HNSW index for distance-based metrics
	Hnsw(HnswParams),
	/// Index with Full-Text search capabilities
	FullText(FullTextParams),
	/// Count index
	Count(Option<Cond>),
	/// DiskANN index for distance-based metrics
	DiskAnn(DiskAnnParams),
}

impl From<sql::index::Index> for Index {
	fn from(v: sql::index::Index) -> Self {
		match v {
			sql::index::Index::Idx => Self::Idx,
			sql::index::Index::Uniq => Self::Uniq,
			sql::index::Index::Hnsw(p) => Self::Hnsw(p),
			sql::index::Index::FullText(p) => Self::FullText(p),
			sql::index::Index::Count(c) => Self::Count(c.map(Into::into)),
			sql::index::Index::DiskAnn(p) => Self::DiskAnn(p),
		}
	}
}

impl From<Index> for sql::index::Index {
	fn from(v: Index) -> Self {
		match v {
			Index::Idx => Self::Idx,
			Index::Uniq => Self::Uniq,
			Index::Hnsw(p) => Self::Hnsw(p),
			Index::FullText(p) => Self::FullText(p),
			Index::Count(c) => Self::Count(c.map(Into::into)),
			Index::DiskAnn(p) => Self::DiskAnn(p),
		}
	}
}

impl ToSql for Index {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: sql::index::Index = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
