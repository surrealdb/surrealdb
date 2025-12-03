use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::Expr;
use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::statements::define::DefineKind;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct EventDefinition {
	pub(crate) name: String,
	pub(crate) target_table: String,
	pub(crate) when: Expr,
	pub(crate) then: Vec<Expr>,
	pub(crate) comment: Option<String>,
}

impl_kv_value_revisioned!(EventDefinition);

impl EventDefinition {
	pub fn to_sql_definition(&self) -> crate::sql::DefineEventStatement {
		crate::sql::DefineEventStatement {
			kind: DefineKind::Default,
			name: crate::sql::Expr::Idiom(crate::sql::Idiom::field(self.name.clone())),
			target_table: crate::sql::Expr::Idiom(crate::sql::Idiom::field(
				self.target_table.clone(),
			)),
			when: self.when.clone().into(),
			then: self.then.iter().cloned().map(Into::into).collect(),
			comment: self
				.comment
				.clone()
				.map(|v| crate::sql::Expr::Literal(crate::sql::Literal::String(v))),
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
