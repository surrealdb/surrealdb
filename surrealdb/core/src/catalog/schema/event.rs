use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::auth::AuthLimit;
use crate::expr::Expr;
use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::statements::define::DefineKind;
use crate::sql::{self};
use crate::val::{TableName, Value};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct EventDefinition {
	pub(crate) name: String,
	pub(crate) target_table: TableName,
	pub(crate) when: Expr,
	pub(crate) then: Vec<Expr>,
	pub(crate) comment: Option<String>,
	/// The auth limit of the API.
	#[revision(end = 2, convert_fn = "readd_auth_limit")]
	pub(crate) old_auth_limit: AuthLimit,
	#[revision(start = 2, default_fn = "existing_auth_limit")]
	pub(crate) auth_limit: AuthLimit,
}

// This was pushed in after the first beta, so we need to add auth_limit to structs in a non-breaking way
impl EventDefinition {
	fn readd_auth_limit(&mut self, _revision: u16, auth_limit: AuthLimit) -> Result<(), revision::Error> {
		self.auth_limit = auth_limit;
		Ok(())
	}

	fn existing_auth_limit(_revision: u16) -> Result<AuthLimit, revision::Error> {
		Ok(AuthLimit::new_no_limit())
	}
}

impl_kv_value_revisioned!(EventDefinition);

impl EventDefinition {
	pub fn to_sql_definition(&self) -> sql::DefineEventStatement {
		sql::DefineEventStatement {
			kind: DefineKind::Default,
			name: sql::Expr::Idiom(sql::Idiom::field(self.name.clone())),
			target_table: sql::Expr::Table(self.target_table.clone().into_string()),
			when: self.when.clone().into(),
			then: self.then.iter().cloned().map(Into::into).collect(),
			comment: self
				.comment
				.clone()
				.map(|v| sql::Expr::Literal(sql::Literal::String(v)))
				.unwrap_or(sql::Expr::Literal(sql::Literal::None)),
		}
	}
}

impl InfoStructure for EventDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.into(),
			"what".to_string() => self.target_table.into(),
			"when".to_string() => self.when.structure(),
			"then".to_string() => self.then.into_iter().map(|x| x.structure()).collect(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}

impl ToSql for EventDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}
