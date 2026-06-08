use revision::revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::statements::info::InfoStructure;
use crate::expr::{Filter, Tokenizer};
use crate::kvs::impl_kv_value_revisioned;
use crate::sql;
use crate::val::{Array, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct AnalyzerDefinition {
	pub name: Strand,
	pub function: Option<Strand>,
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
				.map(|c| sql::Expr::Literal(sql::Literal::String(c.into())))
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
			"name" => Value::String(self.name.clone()),
			"function", if let Some(v) = self.function => Value::String(v),
			"tokenizers", if let Some(v) = self.tokenizers =>
				v.into_iter()
					.map(|t| Value::String(tokenizer_structure_strand(t)))
					.collect::<Array>()
					.into(),
			"filters", if let Some(v) = self.filters =>
				v.into_iter().map(|f| filter_structure_value(&f)).collect::<Array>().into(),
			"comment", if let Some(v) = self.comment => v.into(),
		})
	}
}

#[inline]
fn tokenizer_structure_strand(t: Tokenizer) -> Strand {
	match t {
		Tokenizer::Blank => Strand::new_static("BLANK"),
		Tokenizer::Camel => Strand::new_static("CAMEL"),
		Tokenizer::Class => Strand::new_static("CLASS"),
		Tokenizer::Punct => Strand::new_static("PUNCT"),
	}
}

#[inline]
fn filter_structure_value(f: &Filter) -> Value {
	match f {
		Filter::Ascii => Value::String(Strand::new_static("ASCII")),
		Filter::Lowercase => Value::String(Strand::new_static("LOWERCASE")),
		Filter::Uppercase => Value::String(Strand::new_static("UPPERCASE")),
		_ => {
			let mut s = String::new();
			f.fmt_sql(&mut s, SqlFormat::SingleLine);
			Value::String(Strand::from(s))
		}
	}
}
