use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::ft::analyzer::filter::FilteringStage;
use crate::idx::ft::analyzer::tokenizer::{Tokenizer, Tokens};
use crate::idx::ft::doclength::DocLength;
use crate::idx::ft::offsets::{Offset, OffsetRecords};
use crate::idx::ft::postings::TermFrequency;
use crate::idx::ft::terms::{TermId, Terms};
use crate::sql::statements::DefineAnalyzerStatement;
use crate::sql::tokenizer::Tokenizer as SqlTokenizer;
use crate::sql::Value;
use crate::sql::{Function, Strand};
use async_recursion::async_recursion;
use filter::Filter;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

mod filter;
mod tokenizer;

pub(crate) struct Analyzer {
	function: Option<String>,
	tokenizers: Option<Vec<SqlTokenizer>>,
	filters: Option<Vec<Filter>>,
}

impl From<DefineAnalyzerStatement> for Analyzer {
	fn from(az: DefineAnalyzerStatement) -> Self {
		Self {
			function: az.function.map(|i| i.0),
			tokenizers: az.tokenizers,
			filters: Filter::from(az.filters),
		}
	}
}
impl Analyzer {
	pub(super) async fn extract_terms(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		t: &Terms,
		query_string: String,
	) -> Result<Vec<Option<(TermId, u32)>>, Error> {
		let tokens =
			self.generate_tokens(ctx, opt, txn, FilteringStage::Querying, query_string).await?;
		// We first collect every unique terms
		// as it can contains duplicates
		let mut terms = HashSet::new();
		for token in tokens.list() {
			terms.insert(token);
		}
		// Now we can extract the term ids
		let mut res = Vec::with_capacity(terms.len());
		let mut tx = txn.lock().await;
		for term in terms {
			let opt_term_id = t.get_term_id(&mut tx, tokens.get_token_string(term)?).await?;
			res.push(opt_term_id.map(|tid| (tid, term.get_char_len())));
		}
		Ok(res)
	}

	/// This method is used for indexing.
	/// It will create new term ids for non already existing terms.
	pub(super) async fn extract_terms_with_frequencies(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		terms: &mut Terms,
		field_content: Vec<Value>,
	) -> Result<(DocLength, Vec<(TermId, TermFrequency)>), Error> {
		let mut dl = 0;
		// Let's first collect all the inputs, and collect the tokens.
		// We need to store them because everything after is zero-copy
		let mut inputs = vec![];
		self.analyze_content(ctx, opt, txn, field_content, FilteringStage::Indexing, &mut inputs)
			.await?;
		// We then collect every unique terms and count the frequency
		let mut tf: HashMap<&str, TermFrequency> = HashMap::new();
		for tks in &inputs {
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
		// Now we can resolve the term ids
		let mut tfid = Vec::with_capacity(tf.len());
		let mut tx = txn.lock().await;
		for (t, f) in tf {
			tfid.push((terms.resolve_term_id(&mut tx, t).await?, f));
		}
		Ok((dl, tfid))
	}

	/// This method is used for indexing.
	/// It will create new term ids for non already existing terms.
	pub(super) async fn extract_terms_with_frequencies_with_offsets(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		terms: &mut Terms,
		content: Vec<Value>,
	) -> Result<(DocLength, Vec<(TermId, TermFrequency)>, Vec<(TermId, OffsetRecords)>), Error> {
		let mut dl = 0;
		// Let's first collect all the inputs, and collect the tokens.
		// We need to store them because everything after is zero-copy
		let mut inputs = Vec::with_capacity(content.len());
		self.analyze_content(ctx, opt, txn, content, FilteringStage::Indexing, &mut inputs).await?;
		// We then collect every unique terms and count the frequency and extract the offsets
		let mut tfos: HashMap<&str, Vec<Offset>> = HashMap::new();
		for (i, tks) in inputs.iter().enumerate() {
			for tk in tks.list() {
				dl += 1;
				let s = tks.get_token_string(tk)?;
				let o = tk.new_offset(i as u32);
				match tfos.entry(s) {
					Entry::Vacant(e) => {
						e.insert(vec![o]);
					}
					Entry::Occupied(mut e) => e.get_mut().push(o),
				}
			}
		}

		// Now we can resolve the term ids
		let mut tfid = Vec::with_capacity(tfos.len());
		let mut osid = Vec::with_capacity(tfos.len());
		let mut tx = txn.lock().await;
		for (t, o) in tfos {
			let id = terms.resolve_term_id(&mut tx, t).await?;
			tfid.push((id, o.len() as TermFrequency));
			osid.push((id, OffsetRecords(o)));
		}
		Ok((dl, tfid, osid))
	}

	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	async fn analyze_content(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		content: Vec<Value>,
		stage: FilteringStage,
		tks: &mut Vec<Tokens>,
	) -> Result<(), Error> {
		for v in content {
			self.analyze_value(ctx, opt, txn, v, stage, tks).await?;
		}
		Ok(())
	}

	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	async fn analyze_value(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		val: Value,
		stage: FilteringStage,
		tks: &mut Vec<Tokens>,
	) -> Result<(), Error> {
		match val {
			Value::Strand(s) => tks.push(self.generate_tokens(ctx, opt, txn, stage, s.0).await?),
			Value::Number(n) => {
				tks.push(self.generate_tokens(ctx, opt, txn, stage, n.to_string()).await?)
			}
			Value::Bool(b) => {
				tks.push(self.generate_tokens(ctx, opt, txn, stage, b.to_string()).await?)
			}
			Value::Array(a) => {
				for v in a.0 {
					self.analyze_value(ctx, opt, txn, v, stage, tks).await?;
				}
			}
			Value::Object(o) => {
				for (_, v) in o.0 {
					self.analyze_value(ctx, opt, txn, v, stage, tks).await?;
				}
			}
			_ => {}
		};
		Ok(())
	}

	async fn generate_tokens(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stage: FilteringStage,
		mut input: String,
	) -> Result<Tokens, Error> {
		if let Some(function_name) = self.function.clone() {
			let fns = Function::Custom(function_name.clone(), vec![Value::Strand(Strand(input))]);
			let val = fns.compute(ctx, opt, txn, None).await?;
			if let Value::Strand(val) = val {
				input = val.0;
			} else {
				return Err(Error::InvalidFunction {
					name: function_name,
					message: "The function should return a string.".to_string(),
				});
			}
		}
		if let Some(t) = &self.tokenizers {
			if !input.is_empty() {
				let t = Tokenizer::tokenize(t, input);
				return Filter::apply_filters(t, &self.filters, stage);
			}
		}
		Ok(Tokens::new(input))
	}

	/// Used for exposing the analyzer as the native function `search::analyze`
	pub(crate) async fn analyze(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		input: String,
	) -> Result<Value, Error> {
		self.generate_tokens(ctx, opt, txn, FilteringStage::Indexing, input).await?.try_into()
	}
}

#[cfg(test)]
mod tests {
	use super::Analyzer;
	use crate::ctx::Context;
	use crate::dbs::{Options, Transaction};
	use crate::idx::ft::analyzer::filter::FilteringStage;
	use crate::idx::ft::analyzer::tokenizer::{Token, Tokens};
	use crate::kvs::{Datastore, LockType, TransactionType};
	use crate::{
		sql::{statements::DefineStatement, Statement},
		syn,
	};
	use futures::lock::Mutex;
	use std::sync::Arc;

	async fn get_analyzer_tokens(def: &str, input: &str) -> Tokens {
		let ds = Datastore::new("memory").await.unwrap();
		let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
		let txn: Transaction = Arc::new(Mutex::new(tx));

		let mut stmt = syn::parse(&format!("DEFINE {def}")).unwrap();
		let Some(Statement::Define(DefineStatement::Analyzer(az))) = stmt.0 .0.pop() else {
			panic!()
		};
		let a: Analyzer = az.into();

		a.generate_tokens(
			&Context::default(),
			&Options::default(),
			&txn,
			FilteringStage::Indexing,
			input.to_string(),
		)
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
}
