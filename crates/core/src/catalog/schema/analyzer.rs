use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::statements::info::InfoStructure;
use crate::expr::{Filter, Tokenizer};
use crate::kvs::impl_kv_value_revisioned;
use crate::sql;
use crate::val::{Array, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct AnalyzerDefinition {
	pub name: String,
	pub function: Option<String>,
	pub tokenizers: Option<Vec<Tokenizer>>,
	pub filters: Option<Vec<Filter>>,
	pub comment: Option<String>,
}

impl_kv_value_revisioned!(AnalyzerDefinition);

impl AnalyzerDefinition {
	fn to_sql_definition(&self) -> sql::statements::define::DefineAnalyzerStatement {
		sql::statements::define::DefineAnalyzerStatement {
			kind: sql::statements::define::DefineKind::Default,
			name: sql::Expr::Idiom(sql::Idiom::field(self.name.clone())),
			function: self.function.clone(),
			tokenizers: self.tokenizers.clone().map(|v| v.into_iter().map(|t| t.into()).collect()),
			filters: self.filters.clone().map(|v| v.into_iter().map(|f| f.into()).collect()),
			comment: self
				.comment
				.clone()
				.map(|c| sql::Expr::Literal(sql::Literal::String(c)))
				.unwrap_or(sql::Expr::Literal(sql::Literal::None)),
		}
	}
}

impl ToSql for &AnalyzerDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}

impl InfoStructure for AnalyzerDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => Value::from(self.name.clone()),
			"function".to_string(), if let Some(v) = self.function => Value::from(v.clone()),
			"tokenizers".to_string(), if let Some(v) = self.tokenizers =>
				v.into_iter().map(|v| v.to_string().into()).collect::<Array>().into(),
			"filters".to_string(), if let Some(v) = self.filters =>
				v.into_iter().map(|v| v.to_sql().into()).collect::<Array>().into(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
