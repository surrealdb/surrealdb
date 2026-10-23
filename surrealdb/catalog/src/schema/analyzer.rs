use revision::revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::statements::info::InfoStructure;
use crate::expr::{Filter, Tokenizer};
use crate::stored::Stored;
use crate::val::{Array, Value};
use crate::{expr as runtime, impl_kv_value_catalog};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct AnalyzerDefinition {
	#[stored(n = 2)]
	pub name: Strand,
	#[stored(n = 4)]
	pub function: Option<Strand>,
	#[stored(n = 6)]
	pub tokenizers: Option<Vec<Tokenizer>>,
	#[stored(n = 8)]
	pub filters: Option<Vec<Filter>>,
	#[stored(
		n = 1,
		ignorable,
		independent = "a comment is text for people and no other field reads it"
	)]
	pub comment: Option<String>,
}

impl_kv_value_catalog!(AnalyzerDefinition);

impl AnalyzerDefinition {
	fn to_runtime_definition(&self) -> runtime::statements::define::DefineAnalyzerStatement {
		runtime::statements::define::DefineAnalyzerStatement {
			kind: runtime::statements::define::DefineKind::Default,
			name: runtime::Expr::Idiom(runtime::Idiom::field(self.name.clone())),
			function: self.function.clone(),
			tokenizers: self.tokenizers.clone(),
			filters: self.filters.clone(),
			comment: self
				.comment
				.clone()
				.map(|c| runtime::Expr::Literal(runtime::Literal::String(c.into())))
				.unwrap_or(runtime::Expr::Literal(runtime::Literal::None)),
		}
	}
}

impl ToSql for &AnalyzerDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_runtime_definition().fmt_sql(f, fmt)
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
		Tokenizer::Segment(l) => Strand::new(format!("SEGMENT({l})")),
	}
}

#[inline]
fn filter_structure_value(f: &Filter) -> Value {
	match f {
		Filter::Ascii => Value::String(Strand::new_static("ASCII")),
		Filter::Lowercase => Value::String(Strand::new_static("LOWERCASE")),
		Filter::Uppercase => Value::String(Strand::new_static("UPPERCASE")),
		Filter::EdgeNgram(..) | Filter::Ngram(..) | Filter::Snowball(..) | Filter::Mapper(..) => {
			let mut s = String::new();
			f.fmt_sql(&mut s, SqlFormat::SingleLine);
			Value::String(Strand::from(s))
		}
	}
}
