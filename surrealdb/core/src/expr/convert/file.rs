//! `sql` -> `expr` conversions for [`crate::sql::file`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::file::*;

impl From<File> for crate::val::File {
	fn from(v: File) -> Self {
		Self {
			bucket: v.bucket,
			key: v.key,
		}
	}
}

impl From<crate::val::File> for File {
	fn from(v: crate::val::File) -> Self {
		Self {
			bucket: v.bucket,
			key: v.key,
		}
	}
}
