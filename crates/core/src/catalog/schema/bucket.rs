use revision::revisioned;
use serde::{Deserialize, Serialize};
use surrealdb_types::{ToSql, write_sql};

use crate::catalog::Permission;
use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::statements::define::{DefineBucketStatement, DefineKind};
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct BucketId(pub u32);

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct BucketDefinition {
	pub(crate) id: Option<BucketId>,
	pub(crate) name: String,
	pub(crate) backend: Option<String>,
	pub(crate) permissions: Permission,
	pub(crate) readonly: bool,
	pub(crate) comment: Option<String>,
}
impl_kv_value_revisioned!(BucketDefinition);

impl BucketDefinition {
	pub fn to_sql_definition(&self) -> DefineBucketStatement {
		DefineBucketStatement {
			kind: DefineKind::Default,
			name: crate::sql::Expr::Idiom(crate::sql::Idiom::field(self.name.clone())),
			backend: self
				.backend
				.clone()
				.map(|v| crate::sql::Expr::Literal(crate::sql::Literal::String(v))),
			permissions: self.permissions.clone().into(),
			readonly: self.readonly,
			comment: self
				.comment
				.clone()
				.map(|v| crate::sql::Expr::Literal(crate::sql::Literal::String(v))),
		}
	}
}

impl InfoStructure for BucketDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.into(),
			"permissions".to_string() => self.permissions.structure(),
			"backend".to_string(), if let Some(backend) = self.backend => Value::String(backend.to_string()),
			"readonly".to_string() => self.readonly.into(),
			"comment".to_string(), if let Some(comment) = self.comment => comment.into(),
		})
	}
}

impl ToSql for BucketDefinition {
	fn fmt_sql(&self, f: &mut String) {
		write_sql!(f, "{}", self.to_sql_definition())
	}
}
