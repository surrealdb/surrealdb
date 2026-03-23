use surrealdb_types::{SqlFormat, ToSql};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum RemoveConfigKind {
	#[default]
	GraphQL,
	Api,
	Default,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct RemoveConfigStatement {
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

impl From<RemoveConfigStatement> for crate::expr::statements::remove::RemoveConfigStatement {
	fn from(v: RemoveConfigStatement) -> Self {
		crate::expr::statements::remove::RemoveConfigStatement {
			kind: v.kind.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::remove::RemoveConfigStatement> for RemoveConfigStatement {
	fn from(v: crate::expr::statements::remove::RemoveConfigStatement) -> Self {
		RemoveConfigStatement {
			kind: v.kind.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<RemoveConfigKind> for crate::iam::ConfigKind {
	fn from(v: RemoveConfigKind) -> Self {
		match v {
			RemoveConfigKind::GraphQL => crate::iam::ConfigKind::GraphQL,
			RemoveConfigKind::Api => crate::iam::ConfigKind::Api,
			RemoveConfigKind::Default => crate::iam::ConfigKind::Default,
		}
	}
}

impl From<crate::iam::ConfigKind> for RemoveConfigKind {
	fn from(v: crate::iam::ConfigKind) -> Self {
		match v {
			crate::iam::ConfigKind::GraphQL => RemoveConfigKind::GraphQL,
			crate::iam::ConfigKind::Api => RemoveConfigKind::Api,
			crate::iam::ConfigKind::Default => RemoveConfigKind::Default,
		}
	}
}
