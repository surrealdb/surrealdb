use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use filter::Filter;
use surrealdb_strand::Strand;
use surrealdb_types::ToSql;

use crate::catalog;
use crate::ft::analyzer::filter::FilteringStage;
use crate::ft::analyzer::segmenter::Segmenter;
use crate::ft::analyzer::tokenizer::{Tokenizer, Tokens};
use crate::ft::offset::Offset;
use crate::ft::{DocLength, TermFrequency};
use crate::trees::store::IndexStores;
use crate::val::Value;

pub(in crate::ft) mod filter;
pub(crate) mod mapper;
#[cfg(feature = "cjk-tokenizer")]
pub(crate) mod segmenter;
#[cfg(not(feature = "cjk-tokenizer"))]
#[path = "segmenter_disabled.rs"]
pub(crate) mod segmenter;
pub(in crate::ft) mod tokenizer;

/// A boxed future returned by [`AnalyzerFunction::call`].
///
/// Boxed at the trait boundary because the seam is held as a trait object.
pub type BoxAnalyzerFut<'a> = Pin<Box<dyn Future<Output = Result<Strand>> + Send + 'a>>;

/// Runs the user-defined function named by an analyzer's `FUNCTION` clause.
///
/// The clause names a `fn::`/`mod::` function that pre-processes every piece of
/// text before it is tokenized, and calling one needs the whole execution
/// environment: a context, the statement's options, the capability checks that
/// guard custom functions. Those live above this layer, so the seam is this
/// trait. The implementation *holds* the environment; only the function name
/// and the text cross the boundary.
///
/// `Sync`, and [`BoxAnalyzerFut`] `Send`, because the streaming executor
/// drives the analyzer from a `Send` boxed future and holds a
/// `&dyn AnalyzerFunction` across its awaits.
pub trait AnalyzerFunction: Sync {
	/// Calls the function stored under `name` with `input`, returning the text
	/// it produced.
	///
	/// Errors when the function is not allowed, fails, or returns anything
	/// other than a string.
	fn call<'a>(&'a self, name: &'a str, input: Strand) -> BoxAnalyzerFut<'a>;
}

#[derive(Clone)]
pub struct Analyzer {
	az: Arc<catalog::AnalyzerDefinition>,
	filters: Arc<Option<Vec<Filter>>>,
	/// Resolved from the definition's `SEGMENT(<language>)` tokenizer, when it
	/// declares one. Held beside the definition rather than looked up per call
	/// because resolving it loads a dictionary.
	segmenter: Arc<Option<Segmenter>>,
	/// Without `fulltext` no analyzer exists, so neither does a full-text
	/// index, which holds one.
	#[cfg(not(feature = "fulltext"))]
	disabled: std::convert::Infallible,
}

impl Analyzer {
	/// Async because resolving a `SEGMENT` tokenizer may load a morphological
	/// dictionary, which is slow enough that it runs on the blocking pool and
	/// that concurrent callers wait for one load rather than each doing their
	/// own.
	#[cfg(feature = "fulltext")]
	pub async fn new(ixs: &IndexStores, az: Arc<catalog::AnalyzerDefinition>) -> Result<Self> {
		Ok(Self {
			filters: Arc::new(Filter::try_from(ixs, &az.filters)?),
			segmenter: Arc::new(ixs.segmenters().for_definition(&az).await?),
			az,
		})
	}

	/// Always fails: this build has no full-text support.
	#[cfg(not(feature = "fulltext"))]
	pub async fn new(_ixs: &IndexStores, _az: Arc<catalog::AnalyzerDefinition>) -> Result<Self> {
		Err(anyhow::Error::new(crate::Error::FeatureDisabled {
			feature: "fulltext",
			what: "Analyzers and full-text indexes",
		}))
	}

	pub(in crate::ft) async fn analyze_content(
		&self,
		az_fn: &dyn AnalyzerFunction,
		content: Vec<Value>,
		stage: FilteringStage,
	) -> Result<Vec<Tokens>> {
		let mut tks = Vec::with_capacity(content.len());
		for v in content {
			self.analyze_value(az_fn, v, stage, &mut tks).await?;
		}
		Ok(tks)
	}

	/// Tokenise a single `Value` into `tks`. Strings, numbers, and booleans
	/// contribute their tokens directly; arrays and objects contribute their
	/// elements' tokens, in order; other variants are ignored.
	///
	/// A value's nesting depth is data, not grammar, so nested collections are
	/// walked with an explicit work stack rather than by recursion.
	pub(super) async fn analyze_value(
		&self,
		az_fn: &dyn AnalyzerFunction,
		val: Value,
		stage: FilteringStage,
		tks: &mut Vec<Tokens>,
	) -> Result<()> {
		// Pending values, last-in first-out: a collection pushes its elements
		// in reverse so that they are tokenised in their original order.
		let mut pending = vec![val];
		while let Some(val) = pending.pop() {
			match val {
				Value::String(s) => tks.push(self.generate_tokens(az_fn, stage, s).await?),
				Value::Number(n) => {
					tks.push(self.generate_tokens(az_fn, stage, n.to_sql().into()).await?)
				}
				Value::Bool(b) => {
					tks.push(self.generate_tokens(az_fn, stage, b.to_sql().into()).await?)
				}
				Value::Array(a) => pending.extend(a.0.into_iter().rev()),
				Value::Object(o) => {
					let start = pending.len();
					pending.extend(o.0.into_values());
					pending[start..].reverse();
				}
				_ => {}
			}
		}
		Ok(())
	}

	pub(super) async fn generate_tokens(
		&self,
		az_fn: &dyn AnalyzerFunction,
		stage: FilteringStage,
		input: Strand,
	) -> Result<Tokens> {
		// Only an analyzer that declares a `FUNCTION` reaches the seam; without
		// one the input is tokenized exactly as the caller supplied it, so no
		// evaluator is ever consulted.
		let input = match self.az.function.as_deref() {
			Some(function_name) => az_fn.call(function_name, input).await?,
			None => input,
		};
		if input.is_empty() {
			return Ok(Tokens::new(input));
		}

		let segmenter = self.segmenter.as_ref().as_ref();
		let tokens = match &self.az.tokenizers {
			Some(t) => Tokenizer::tokenize(t, segmenter, input)?,
			None => Tokenizer::tokenize(&[], segmenter, input)?,
		};
		Filter::apply_filters(tokens, &self.filters, stage)
	}

	/// Used for exposing the analyzer as the native function `search::analyze`
	pub async fn analyze(&self, az_fn: &dyn AnalyzerFunction, input: Strand) -> Result<Value> {
		self.generate_tokens(az_fn, FilteringStage::Indexing, input).await?.try_into()
	}

	pub(in crate::ft) fn extract_frequencies(
		inputs: &[Tokens],
	) -> Result<(DocLength, HashMap<&str, TermFrequency>)> {
		let mut dl = 0;
		let mut tf: HashMap<&str, TermFrequency> = HashMap::new();
		for tks in inputs {
			for tk in tks.list() {
				dl += 1;
				let s = tks.get_token_string(tk)?;
				match tf.entry(s) {
					Entry::Vacant(e) => {
						e.insert(1);
					}
					Entry::Occupied(mut e) => {
						e.insert(*e.get() + 1);
					}
				}
			}
		}
		Ok((dl, tf))
	}

	pub(in crate::ft) fn extract_offsets(
		inputs: &[Tokens],
	) -> anyhow::Result<(DocLength, HashMap<&str, Vec<Offset>>)> {
		let mut dl = 0;
		let mut tfos: HashMap<&str, Vec<Offset>> = HashMap::new();
		for (i, tks) in inputs.iter().enumerate() {
			for tk in tks.list() {
				dl += 1;
				let s = tks.get_token_string(tk)?;
				let o = tk.new_offset(i as u32);
				tfos.entry(s).or_default().push(o);
			}
		}
		Ok((dl, tfos))
	}
}

#[cfg(test)]
#[cfg(feature = "fulltext")]
mod tests {
	use std::sync::Arc;

	use surrealdb_kvs::TransactionType;
	use surrealdb_sql::Expr;
	use surrealdb_sql::statements::DefineStatement;
	use surrealdb_strand::Strand;
	use surrealdb_syn as syn;

	use super::{Analyzer, AnalyzerFunction, BoxAnalyzerFut};
	use crate::catalog::AnalyzerDefinition;
	use crate::expr::DefineAnalyzerStatement;
	use crate::ft::analyzer::filter::FilteringStage;
	use crate::ft::analyzer::tokenizer::{Token, Tokens};
	use crate::test_env::TestIndexStore;

	/// Stands in for the evaluator behind an analyzer's `FUNCTION` clause. No
	/// definition below declares one, so the seam is never reached.
	struct NoAnalyzerFunction;

	impl AnalyzerFunction for NoAnalyzerFunction {
		fn call<'a>(&'a self, name: &'a str, _input: Strand) -> BoxAnalyzerFut<'a> {
			unreachable!("no analyzer under test declares FUNCTION {name}")
		}
	}

	/// The analyzer definition a `DEFINE ANALYZER` clause describes.
	///
	/// The analyzer consumes the definition, not the statement: turning one into
	/// the other is the evaluator's job, and every field the analyzer reads is
	/// carried across unchanged. `name` is not one of them — nothing on the
	/// tokenizing path reads it — so it is fixed to the name every clause here
	/// uses.
	fn analyzer_definition(def: &str) -> AnalyzerDefinition {
		let Expr::Define(d) = syn::expr(&format!("DEFINE {def}")).unwrap() else {
			panic!()
		};
		let DefineStatement::Analyzer(az) = *d else {
			panic!()
		};
		let az = DefineAnalyzerStatement::from(az);
		AnalyzerDefinition {
			name: "test".into(),
			function: az.function,
			tokenizers: az.tokenizers,
			filters: az.filters,
			comment: None,
		}
	}

	async fn get_analyzer_tokens(def: &str, input: &str) -> Tokens {
		let ds = TestIndexStore::new().await;
		let tx = ds.transaction(TransactionType::Read).await.unwrap();

		let a = Analyzer::new(ds.index_stores(), Arc::new(analyzer_definition(def))).await.unwrap();
		let az_fn = NoAnalyzerFunction;
		let tokens =
			a.generate_tokens(&az_fn, FilteringStage::Indexing, input.into()).await.unwrap();
		tx.cancel().await.unwrap();
		tokens
	}

	pub(super) async fn test_analyzer(def: &str, input: &str, expected: &[&str]) {
		let tokens = get_analyzer_tokens(def, input).await;
		let mut res = vec![];
		for t in tokens.list() {
			res.push(tokens.get_token_string(t).unwrap());
		}
		assert_eq!(&res, expected);
	}

	pub(super) async fn test_analyzer_tokens(def: &str, input: &str, expected: &[Token]) {
		let tokens = get_analyzer_tokens(def, input).await;
		assert_eq!(tokens.list(), expected);
	}

	#[tokio::test]
	async fn test_no_tokenizer() {
		test_analyzer("ANALYZER test FILTERS lowercase", "ab", &["ab"]).await;
	}

	/// Collections contribute their elements' tokens in document order, and a
	/// value's nesting depth — which is data, not grammar — is walked without
	/// recursion, so a deeply nested value cannot exhaust the stack.
	#[tokio::test]
	async fn analyze_value_walks_nested_collections_in_order() {
		use std::collections::BTreeMap;

		use crate::val::{Array, Object, Value};

		let ds = TestIndexStore::new().await;
		let a = Analyzer::new(ds.index_stores(), Arc::new(analyzer_definition("ANALYZER test")))
			.await
			.unwrap();
		let words = |tks: &[Tokens]| -> Vec<String> {
			tks.iter()
				.flat_map(|t| t.list().iter().map(|x| t.get_token_string(x).unwrap().to_owned()))
				.collect()
		};

		let object = Object::from(BTreeMap::from([
			("a", Value::from("b")),
			("c", Value::Array(Array(vec![Value::from("d"), Value::from("e")]))),
		]));
		let value = Value::Array(Array(vec![
			Value::from("a"),
			Value::Object(object),
			Value::Array(Array(vec![Value::from("f")])),
			Value::from("g"),
		]));
		let mut tks = Vec::new();
		a.analyze_value(&NoAnalyzerFunction, value, FilteringStage::Indexing, &mut tks)
			.await
			.unwrap();
		assert_eq!(words(&tks), ["a", "b", "d", "e", "f", "g"]);

		let mut deep = Value::from("leaf");
		for _ in 0..100_000 {
			deep = Value::Array(Array(vec![deep]));
		}
		let mut tks = Vec::new();
		a.analyze_value(&NoAnalyzerFunction, deep, FilteringStage::Indexing, &mut tks)
			.await
			.unwrap();
		assert_eq!(words(&tks), ["leaf"]);
	}
}
