use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use anyhow::{Result, bail};
use filter::Filter;
use reblessive::tree::Stk;

use crate::catalog;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::{FlowResultExt as _, Function};
use crate::idx::ft::analyzer::filter::FilteringStage;
use crate::idx::ft::analyzer::tokenizer::{Tokenizer, Tokens};
use crate::idx::ft::offset::Offset;
use crate::idx::ft::{DocLength, TermFrequency};
use crate::idx::trees::store::IndexStores;
use crate::val::{Strand, Value};

pub(in crate::idx::ft) mod filter;
pub(in crate::idx) mod mapper;
pub(in crate::idx::ft) mod tokenizer;

#[derive(Clone)]
pub(crate) struct Analyzer {
	az: Arc<catalog::AnalyzerDefinition>,
	filters: Arc<Option<Vec<Filter>>>,
}

impl Analyzer {
	pub(crate) fn new(ixs: &IndexStores, az: Arc<catalog::AnalyzerDefinition>) -> Result<Self> {
		Ok(Self {
			filters: Arc::new(Filter::try_from(ixs, &az.filters)?),
			az,
		})
	}

	pub(in crate::idx::ft) async fn analyze_content(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		content: Vec<Value>,
		stage: FilteringStage,
	) -> Result<Vec<Tokens>> {
		let mut tks = Vec::with_capacity(content.len());
		for v in content {
			self.analyze_value(stk, ctx, opt, v, stage, &mut tks).await?;
		}
		Ok(tks)
	}

	/// Was marked recursive
	pub(super) async fn analyze_value(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		val: Value,
		stage: FilteringStage,
		tks: &mut Vec<Tokens>,
	) -> Result<()> {
		match val {
			Value::Strand(s) => {
				tks.push(self.generate_tokens(stk, ctx, opt, stage, s.into_string()).await?)
			}
			Value::Number(n) => {
				tks.push(self.generate_tokens(stk, ctx, opt, stage, n.to_string()).await?)
			}
			Value::Bool(b) => {
				tks.push(self.generate_tokens(stk, ctx, opt, stage, b.to_string()).await?)
			}
			Value::Array(a) => {
				for v in a.0 {
					stk.run(|stk| self.analyze_value(stk, ctx, opt, v, stage, tks)).await?;
				}
			}
			Value::Object(o) => {
				for (_, v) in o.0 {
					stk.run(|stk| self.analyze_value(stk, ctx, opt, v, stage, tks)).await?;
				}
			}
			_ => {}
		};
		Ok(())
	}

	pub(super) async fn generate_tokens(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stage: FilteringStage,
		mut input: String,
	) -> Result<Tokens> {
		if let Some(function_name) = self.az.function.as_ref().map(|i| i.as_str().to_owned()) {
			let val = Function::Custom(function_name.clone())
				// TODO: Null byte check
				.compute(
					stk,
					ctx,
					opt,
					None,
					vec![Value::Strand(unsafe { Strand::new_unchecked(input) })],
				)
				.await
				.catch_return()?;
			if let Value::Strand(val) = val {
				input = val.into_string();
			} else {
				bail!(Error::InvalidFunction {
					name: function_name,
					message: "The function should return a string.".to_string(),
				});
			}
		}
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
	pub(crate) async fn analyze(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		input: String,
	) -> Result<Value> {
		self.generate_tokens(stk, ctx, opt, FilteringStage::Indexing, input).await?.try_into()
	}

	pub(in crate::idx::ft) fn extract_frequencies(
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

	pub(in crate::idx::ft) fn extract_offsets(
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

	use super::Analyzer;
	use crate::ctx::MutableContext;
	use crate::dbs::Options;
	use crate::expr::DefineAnalyzerStatement;
	use crate::idx::ft::analyzer::filter::FilteringStage;
	use crate::idx::ft::analyzer::tokenizer::{Token, Tokens};
	use crate::kvs::{Datastore, LockType, TransactionType};
	use crate::sql::{DefineStatement, Expr};
	use crate::syn;

	async fn get_analyzer_tokens(def: &str, input: &str) -> Tokens {
		let ds = Datastore::new("memory").await.unwrap();
		let txn = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
		let mut ctx = MutableContext::default();
		ctx.set_transaction(Arc::new(txn));
		let ctx = ctx.freeze();

		let expr = syn::expr(&format!("DEFINE {def}")).unwrap();
		let Expr::Define(d) = expr else {
			panic!()
		};
		let DefineStatement::Analyzer(az) = *d else {
			panic!()
		};

		let a = Analyzer::new(
			ctx.get_index_stores(),
			Arc::new(DefineAnalyzerStatement::from(az).to_definition()),
		)
		.unwrap();

		let mut stack = reblessive::TreeStack::new();

		let opts = Options::default();
		stack
			.enter(|stk| {
				a.generate_tokens(stk, &ctx, &opts, FilteringStage::Indexing, input.to_string())
			})
			.finish()
			.await
			.unwrap()
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
