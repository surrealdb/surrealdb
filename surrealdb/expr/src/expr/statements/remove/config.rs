use surrealdb_types::{SqlFormat, ToSql};

use crate::iam::ConfigKind;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RemoveConfigStatement {
	pub kind: ConfigKind,
	pub if_exists: bool,
}

impl ToSql for RemoveConfigStatement {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("REMOVE CONFIG");
		if self.if_exists {
			f.push_str(" IF EXISTS");
		}
		match self.kind {
			ConfigKind::GraphQL => f.push_str(" GRAPHQL"),
			ConfigKind::Api => f.push_str(" API"),
			ConfigKind::Default => f.push_str(" DEFAULT"),
		}
	}
}
