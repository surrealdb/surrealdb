//! `sql` -> `expr` conversions for [`crate::sql::record_id::key`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::record_id::key::*;

impl From<RecordIdKeyGen> for crate::expr::RecordIdKeyGen {
	fn from(value: RecordIdKeyGen) -> Self {
		match value {
			RecordIdKeyGen::Rand => crate::expr::RecordIdKeyGen::Rand,
			RecordIdKeyGen::Ulid => crate::expr::RecordIdKeyGen::Ulid,
			RecordIdKeyGen::Uuid => crate::expr::RecordIdKeyGen::Uuid,
		}
	}
}

impl From<crate::expr::RecordIdKeyGen> for RecordIdKeyGen {
	fn from(value: crate::expr::RecordIdKeyGen) -> Self {
		match value {
			crate::expr::RecordIdKeyGen::Rand => RecordIdKeyGen::Rand,
			crate::expr::RecordIdKeyGen::Ulid => RecordIdKeyGen::Ulid,
			crate::expr::RecordIdKeyGen::Uuid => RecordIdKeyGen::Uuid,
		}
	}
}

impl From<RecordIdKeyLit> for crate::expr::RecordIdKeyLit {
	fn from(value: RecordIdKeyLit) -> Self {
		match value {
			RecordIdKeyLit::Number(x) => crate::expr::RecordIdKeyLit::Number(x),
			RecordIdKeyLit::String(x) => crate::expr::RecordIdKeyLit::String(x),
			RecordIdKeyLit::Uuid(x) => crate::expr::RecordIdKeyLit::Uuid(crate::val::Uuid(x)),
			RecordIdKeyLit::Array(x) => {
				crate::expr::RecordIdKeyLit::Array(x.into_iter().map(From::from).collect())
			}
			RecordIdKeyLit::Object(x) => {
				crate::expr::RecordIdKeyLit::Object(x.into_iter().map(From::from).collect())
			}
			RecordIdKeyLit::Generate(x) => crate::expr::RecordIdKeyLit::Generate(x.into()),
			RecordIdKeyLit::Range(x) => crate::expr::RecordIdKeyLit::Range(Box::new((*x).into())),
		}
	}
}

impl From<crate::expr::RecordIdKeyLit> for RecordIdKeyLit {
	fn from(value: crate::expr::RecordIdKeyLit) -> Self {
		match value {
			crate::expr::RecordIdKeyLit::Number(x) => RecordIdKeyLit::Number(x),
			crate::expr::RecordIdKeyLit::String(x) => RecordIdKeyLit::String(x),
			crate::expr::RecordIdKeyLit::Uuid(uuid) => RecordIdKeyLit::Uuid(uuid.0),
			crate::expr::RecordIdKeyLit::Array(exprs) => {
				RecordIdKeyLit::Array(exprs.into_iter().map(From::from).collect())
			}
			crate::expr::RecordIdKeyLit::Object(items) => {
				RecordIdKeyLit::Object(items.into_iter().map(From::from).collect())
			}
			crate::expr::RecordIdKeyLit::Generate(x) => RecordIdKeyLit::Generate(x.into()),
			crate::expr::RecordIdKeyLit::Range(x) => RecordIdKeyLit::Range(Box::new((*x).into())),
		}
	}
}
