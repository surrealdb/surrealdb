//! The parsed index-parameter clause of a DEFINE INDEX statement.
//!
//! Mirrors the catalog's `Index` with the COUNT condition held as a parsed
//! expression rather than canonical text; lowering to a definition renders it
//! through the catalog's stored-text funnel.

use common::fmt::EscapeKwFreeIdent;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::Cond;
use crate::expr::index::{DiskAnnParams, FullTextParams, HnswParams};

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

impl ToSql for Index {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Idx => {}
			Self::Uniq => f.push_str("UNIQUE"),
			Self::Count(c) => {
				f.push_str("COUNT");
				if let Some(v) = c {
					write_sql!(f, fmt, " {}", v)
				}
			}
			Self::FullText(p) => {
				write_sql!(f, fmt, "FULLTEXT ANALYZER {} {}", EscapeKwFreeIdent(&p.az), p.sc);
				if p.hl {
					f.push_str(" HIGHLIGHTS")
				}
			}
			Self::Hnsw(p) => {
				write_sql!(
					f,
					fmt,
					"HNSW DIMENSION {} DIST {} TYPE {} EFC {} M {} M0 {} LM {}",
					p.dimension,
					p.distance,
					p.vector_type,
					p.ef_construction,
					p.m,
					p.m0,
					p.ml
				);
				if p.extend_candidates {
					f.push_str(" EXTEND_CANDIDATES")
				}
				if p.keep_pruned_connections {
					f.push_str(" KEEP_PRUNED_CONNECTIONS")
				}
				if p.use_hashed_vector {
					f.push_str(" HASHED_VECTOR")
				}
			}
			Self::DiskAnn(p) => {
				write_sql!(
					f,
					fmt,
					"DISKANN DIMENSION {} DIST {} TYPE {} DEGREE {} L_BUILD {} ALPHA {}",
					p.dimension,
					p.distance,
					p.vector_type,
					p.degree,
					p.l_build,
					p.alpha
				);
				if p.use_hashed_vector {
					f.push_str(" HASHED_VECTOR")
				}
			}
		}
	}
}
