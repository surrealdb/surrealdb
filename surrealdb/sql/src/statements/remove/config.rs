use surrealdb_types::{SqlFormat, ToSql};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum RemoveConfigKind {
	#[default]
	GraphQL,
	Api,
	Default,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveConfigStatement {
	pub kind: RemoveConfigKind,
	pub if_exists: bool,
}

impl ToSql for RemoveConfigStatement {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("REMOVE CONFIG");
		if self.if_exists {
			f.push_str(" IF EXISTS");
		}
		match self.kind {
			RemoveConfigKind::GraphQL => f.push_str(" GRAPHQL"),
			RemoveConfigKind::Api => f.push_str(" API"),
			RemoveConfigKind::Default => f.push_str(" DEFAULT"),
		}
	}
}

impl From<RemoveConfigKind> for surrealdb_iam::ConfigKind {
	fn from(v: RemoveConfigKind) -> Self {
		match v {
			RemoveConfigKind::GraphQL => surrealdb_iam::ConfigKind::GraphQL,
			RemoveConfigKind::Api => surrealdb_iam::ConfigKind::Api,
			RemoveConfigKind::Default => surrealdb_iam::ConfigKind::Default,
		}
	}
}

impl From<surrealdb_iam::ConfigKind> for RemoveConfigKind {
	fn from(v: surrealdb_iam::ConfigKind) -> Self {
		match v {
			surrealdb_iam::ConfigKind::GraphQL => RemoveConfigKind::GraphQL,
			surrealdb_iam::ConfigKind::Api => RemoveConfigKind::Api,
			surrealdb_iam::ConfigKind::Default => RemoveConfigKind::Default,
		}
	}
}
