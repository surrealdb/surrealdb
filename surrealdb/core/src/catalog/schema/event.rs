use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::auth::AuthLimit;
use crate::expr::Expr;
use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::statements::define::DefineKind;
use crate::sql::{self};
use crate::val::{TableName, Value};

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct EventDefinition {
	pub(crate) name: String,
	pub(crate) target_table: TableName,
	pub(crate) when: Expr,
	pub(crate) then: Vec<Expr>,
	pub(crate) comment: Option<String>,
	/// The auth limit of the API.
	#[revision(start = 2, default_fn = "default_auth_limit")]
	pub(crate) auth_limit: AuthLimit,
	/// Whether this event should be queued for async processing.
	#[revision(start = 3)]
	pub(crate) asynchronous: bool,
	/// Retry limit for async events (values = 0 mean a single attempt).
	#[revision(start = 3, default_fn = "default_retry")]
	pub(crate) retry: u16,
	/// Maximum async event nesting depth for this event.
	#[revision(start = 3, default_fn = "default_max_depth")]
	pub(crate) max_depth: u16,
}

// This was pushed in after the first beta, so we need to add auth_limit to structs in a
// non-breaking way
impl EventDefinition {
	fn default_auth_limit(_revision: u16) -> Result<AuthLimit, revision::Error> {
		Ok(AuthLimit::new_no_limit())
	}

	fn default_retry(_revision: u16) -> Result<u16, revision::Error> {
		Ok(1)
	}

	fn default_max_depth(_revision: u16) -> Result<u16, revision::Error> {
		Ok(5)
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
			asynchronous: self.asynchronous,
			retry: Some(self.retry),
			max_depth: Some(self.max_depth),
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
			"async".to_string(), if self.asynchronous => Value::Bool(true),
			"retry".to_string() =>  self.retry.into(),
			"maxdepth".to_string() => self.max_depth.into(),
		})
	}
}

impl ToSql for EventDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}
