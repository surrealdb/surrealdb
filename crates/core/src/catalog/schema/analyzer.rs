use revision::revisioned;

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

impl InfoStructure for AnalyzerDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => Value::from(self.name.clone()),
			// TODO: Null byte validity
			"function".to_string(), if let Some(v) = self.function => Value::from(v.clone()),
			"tokenizers".to_string(), if let Some(v) = self.tokenizers =>
				v.into_iter().map(|v| v.to_string().into()).collect::<Array>().into(),
			"filters".to_string(), if let Some(v) = self.filters =>
				v.into_iter().map(|v| v.to_string().into()).collect::<Array>().into(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
