use revision::revisioned;
use surrealdb_types::sql::ToSql;

use crate::expr::statements::info::InfoStructure;
use crate::expr::{Filter, Tokenizer};
use crate::kvs::impl_kv_value_revisioned;
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
	fn to_sql_definition(&self) -> crate::sql::statements::define::DefineAnalyzerStatement {
		crate::sql::statements::define::DefineAnalyzerStatement {
			kind: crate::sql::statements::define::DefineKind::Default,
			name: crate::sql::Expr::Idiom(crate::sql::Idiom::field(self.name.clone())),
			function: self.function.clone(),
			tokenizers: self.tokenizers.clone().map(|v| v.into_iter().map(|t| t.into()).collect()),
			filters: self.filters.clone().map(|v| v.into_iter().map(|f| f.into()).collect()),
			comment: self
				.comment
				.clone()
				.map(|c| crate::sql::Expr::Literal(crate::sql::Literal::String(c))),
		}
	}
}

impl ToSql for &AnalyzerDefinition {
	fn fmt_sql(&self, f: &mut String) {
		f.push_str(&self.to_sql_definition().to_string());
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
				v.into_iter().map(|v| v.to_string().into()).collect::<Array>().into(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
