use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use filter::Filter;
use reblessive::tree::Stk;
use surrealdb_strand::Strand;
use surrealdb_types::ToSql;

use crate::catalog;
use crate::ft::analyzer::filter::FilteringStage;
use crate::ft::analyzer::tokenizer::{Tokenizer, Tokens};
use crate::ft::offset::Offset;
use crate::ft::{DocLength, TermFrequency};
use crate::trees::store::IndexStores;
use crate::val::Value;

pub(in crate::ft) mod filter;
pub(crate) mod mapper;
pub(in crate::ft) mod tokenizer;

/// A boxed future returned by [`AnalyzerFunction::call`].
///
/// Boxed at the trait boundary because the seam is held as a trait object.
/// Deliberately not `Send`: the call borrows the caller's `Stk`, and a future
/// holding one cannot be.
pub type BoxAnalyzerFut<'a> = Pin<Box<dyn Future<Output = Result<Strand>> + 'a>>;

/// Runs the user-defined function named by an analyzer's `FUNCTION` clause.
///
/// The clause names a `fn::`/`mod::` function that pre-processes every piece of
/// text before it is tokenized, and calling one needs the whole execution
/// environment: a context, the statement's options, the capability checks that
/// guard custom functions. Those live above this layer, so the seam is this
/// trait. The implementation *holds* the environment; only the function name
/// and the text cross the boundary.
///
/// No auto-trait bound: tokenizing always happens inside a heap-allocated
/// `TreeStack`, which erases auto-traits, so neither the trait object nor
/// [`BoxAnalyzerFut`] ever has to be `Send` — not even where the streaming
/// executor drives the analyzer from a `Send` boxed future. Add `Sync` only if
/// a caller ever needs to hold a `&dyn AnalyzerFunction` across an await
/// outside such a stack.
pub trait AnalyzerFunction {
	/// Calls the function stored under `name` with `input`, returning the text
	/// it produced.
	///
	/// `stk` stays an explicit parameter rather than being captured: the caller
	/// is already inside a `TreeStack` frame and must hand down the same one,
	/// or the function body would build a second stack instead of growing the
	/// first.
	///
	/// Errors when the function is not allowed, fails, or returns anything
	/// other than a string.
	fn call<'a>(&'a self, stk: &'a mut Stk, name: &'a str, input: Strand) -> BoxAnalyzerFut<'a>;
}

#[derive(Clone)]
pub struct Analyzer {
	az: Arc<catalog::AnalyzerDefinition>,
	filters: Arc<Option<Vec<Filter>>>,
}

impl Analyzer {
	pub fn new(ixs: &IndexStores, az: Arc<catalog::AnalyzerDefinition>) -> Result<Self> {
		Ok(Self {
			filters: Arc::new(Filter::try_from(ixs, &az.filters)?),
			az,
		})
	}

	pub(in crate::ft) async fn analyze_content(
		&self,
		stk: &mut Stk,
		az_fn: &dyn AnalyzerFunction,
		content: Vec<Value>,
		stage: FilteringStage,
	) -> Result<Vec<Tokens>> {
		let mut tks = Vec::with_capacity(content.len());
		for v in content {
			self.analyze_value(stk, az_fn, v, stage, &mut tks).await?;
		}
		Ok(tks)
	}

	/// Tokenise a single `Value` into `tks`. Strings, numbers, and booleans
	/// contribute their tokens directly; arrays and objects recurse into their
	/// elements; other variants are ignored.
	pub(super) async fn analyze_value(
		&self,
		stk: &mut Stk,
		az_fn: &dyn AnalyzerFunction,
		val: Value,
		stage: FilteringStage,
		tks: &mut Vec<Tokens>,
	) -> Result<()> {
		match val {
			Value::String(s) => tks.push(self.generate_tokens(stk, az_fn, stage, s).await?),
			Value::Number(n) => {
				tks.push(self.generate_tokens(stk, az_fn, stage, n.to_sql().into()).await?)
			}
			Value::Bool(b) => {
				tks.push(self.generate_tokens(stk, az_fn, stage, b.to_sql().into()).await?)
			}
			Value::Array(a) => {
				for v in a.0 {
					stk.run(|stk| self.analyze_value(stk, az_fn, v, stage, tks)).await?;
				}
			}
			Value::Object(o) => {
				for (_, v) in o.0 {
					stk.run(|stk| self.analyze_value(stk, az_fn, v, stage, tks)).await?;
				}
			}
			_ => {}
		};
		Ok(())
	}

	pub(super) async fn generate_tokens(
		&self,
		stk: &mut Stk,
		az_fn: &dyn AnalyzerFunction,
		stage: FilteringStage,
		input: Strand,
	) -> Result<Tokens> {
		// Only an analyzer that declares a `FUNCTION` reaches the seam; without
		// one the input is tokenized exactly as the caller supplied it, so no
		// evaluator is ever consulted.
		let input = match self.az.function.as_deref() {
			Some(function_name) => az_fn.call(stk, function_name, input).await?,
			None => input,
		};
		if input.is_empty() {
			return Ok(Tokens::new(input));
		}

		let tokens = if let Some(t) = &self.az.tokenizers {
			Tokenizer::tokenize(t, input)
		} else {
			Tokenizer::tokenize(&[], input)
		};
		Filter::apply_filters(tokens, &self.filters, stage)
	}

	/// Used for exposing the analyzer as the native function `search::analyze`
	pub async fn analyze(
		&self,
		stk: &mut Stk,
		az_fn: &dyn AnalyzerFunction,
		input: Strand,
	) -> Result<Value> {
		self.generate_tokens(stk, az_fn, FilteringStage::Indexing, input).await?.try_into()
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
mod tests {
	use std::sync::Arc;

	use reblessive::tree::Stk;
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
		fn call<'a>(
			&'a self,
			_stk: &'a mut Stk,
			name: &'a str,
			_input: Strand,
		) -> BoxAnalyzerFut<'a> {
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

		let a = Analyzer::new(ds.index_stores(), Arc::new(analyzer_definition(def))).unwrap();
		let az_fn = NoAnalyzerFunction;
		let mut stack = reblessive::TreeStack::new();
		let tokens = stack
			.enter(|stk| async move {
				a.generate_tokens(stk, &az_fn, FilteringStage::Indexing, input.into()).await
			})
			.finish()
			.await
			.unwrap();
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
}
