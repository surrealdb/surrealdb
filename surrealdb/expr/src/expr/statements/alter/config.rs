use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::expr::statements::define::config::ConfigInner;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AlterConfigStatement {
	pub if_exists: bool,
	pub inner: ConfigInner,
	pub comment: AlterKind<String>,
}

impl Default for AlterConfigStatement {
	fn default() -> Self {
		Self {
			if_exists: false,
			inner: ConfigInner::GraphQL(Default::default()),
			comment: AlterKind::None,
		}
	}
}

impl ToSql for AlterConfigStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterConfigStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
