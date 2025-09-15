use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::catalog::Permission;
use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::ToSql;
use crate::sql::statements::define::{DefineBucketStatement, DefineKind};
use crate::val::{Strand, Value};

#[revisioned(revision = 1)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct BucketId(pub u32);

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct BucketDefinition {
	pub id: Option<BucketId>,
	pub name: String,
	pub backend: Option<String>,
	pub permissions: Permission,
	pub readonly: bool,
	pub comment: Option<String>,
}
impl_kv_value_revisioned!(BucketDefinition);

impl BucketDefinition {
	pub fn to_sql_definition(&self) -> DefineBucketStatement {
		DefineBucketStatement {
			kind: DefineKind::Default,
			name: unsafe { crate::sql::Ident::new_unchecked(self.name.clone()) },
			backend: self.backend.clone().map(|v| {
				crate::sql::Expr::Literal(crate::sql::Literal::Strand(unsafe {
					Strand::new_unchecked(v)
				}))
			}),
			permissions: self.permissions.clone().into(),
			readonly: self.readonly,
			comment: self.comment.clone().map(Into::into),
		}
	}
}

impl InfoStructure for BucketDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.into(),
			"permissions".to_string() => self.permissions.structure(),
			// TODO: Null byte validity
			"backend".to_string(), if let Some(backend) = self.backend => Value::Strand(Strand::new(backend.to_string()).unwrap()),
			"readonly".to_string() => self.readonly.into(),
			"comment".to_string(), if let Some(comment) = self.comment => comment.into(),
		})
	}
}

impl ToSql for BucketDefinition {
	fn to_sql(&self) -> String {
		self.to_sql_definition().to_string()
	}
}
