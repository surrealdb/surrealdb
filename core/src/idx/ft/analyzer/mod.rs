use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::idx::ft::analyzer::filter::FilteringStage;
use crate::idx::ft::analyzer::tokenizer::{Tokenizer, Tokens};
use crate::idx::ft::doclength::DocLength;
use crate::idx::ft::offsets::{Offset, OffsetRecords};
use crate::idx::ft::postings::TermFrequency;
use crate::idx::ft::terms::{TermId, TermLen, Terms};
use crate::sql::statements::DefineAnalyzerStatement;
use crate::sql::tokenizer::Tokenizer as SqlTokenizer;
use crate::sql::Function;
use crate::sql::Value;
use filter::Filter;
use reblessive::tree::Stk;
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

pub(in crate::idx) type TermsList = Vec<Option<(TermId, TermLen)>>;

pub(in crate::idx) struct TermsSet {
	set: HashSet<TermId>,
	has_unknown_terms: bool,
}

impl TermsSet {
	/// If the query TermsSet contains terms that are unknown in the index
	/// of if there is no terms in the set then
	/// we are sure that it does not match any document
	pub(in crate::idx) fn is_matchable(&self) -> bool {
		!(self.has_unknown_terms || self.set.is_empty())
	}

	pub(in crate::idx) fn is_subset(&self, other: &TermsSet) -> bool {
		if self.has_unknown_terms {
			return false;
		}
		self.set.is_subset(&other.set)
	}
}

impl Analyzer {
	pub(super) async fn extract_querying_terms(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		t: &Terms,
		content: String,
	) -> Result<(TermsList, TermsSet), Error> {
		let tokens = self.generate_tokens(stk, ctx, opt, FilteringStage::Querying, content).await?;
		// We extract the term ids
		let mut list = Vec::with_capacity(tokens.list().len());
		let mut unique_tokens = HashSet::new();
		let mut set = HashSet::new();
		let mut tx = ctx.tx_lock().await;
		let mut has_unknown_terms = false;
		for token in tokens.list() {
			// Tokens can contains duplicated, not need to evaluate them again
			if unique_tokens.insert(token) {
				// Is the term known in the index?
				let opt_term_id = t.get_term_id(&mut tx, tokens.get_token_string(token)?).await?;
				list.push(opt_term_id.map(|tid| (tid, token.get_char_len())));
				if let Some(term_id) = opt_term_id {
					set.insert(term_id);
				} else {
					has_unknown_terms = true;
				}
			}
		}
		drop(tx);
		Ok((
			list,
			TermsSet {
				set,
				has_unknown_terms,
			},
		))
	}

	pub(in crate::idx) async fn extract_indexing_terms(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		t: &Terms,
		content: Value,
	) -> Result<TermsSet, Error> {
		let mut tv = Vec::new();
		self.analyze_value(stk, ctx, opt, content, FilteringStage::Indexing, &mut tv).await?;
		let mut set = HashSet::new();
		let mut has_unknown_terms = false;
		let mut tx = ctx.tx_lock().await;
		for tokens in tv {
			for token in tokens.list() {
				if let Some(term_id) =
					t.get_term_id(&mut tx, tokens.get_token_string(token)?).await?
				{
					set.insert(term_id);
				} else {
					has_unknown_terms = true;
				}
			}
		}
		drop(tx);
		Ok(TermsSet {
			set,
			has_unknown_terms,
		})
	}

	/// This method is used for indexing.
	/// It will create new term ids for non already existing terms.
	pub(super) async fn extract_terms_with_frequencies(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		terms: &mut Terms,
		field_content: Vec<Value>,
	) -> Result<(DocLength, Vec<(TermId, TermFrequency)>), Error> {
		let mut dl = 0;
		// Let's first collect all the inputs, and collect the tokens.
		// We need to store them because everything after is zero-copy
		let mut inputs = vec![];
		self.analyze_content(stk, ctx, opt, field_content, FilteringStage::Indexing, &mut inputs)
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
		let mut tx = ctx.tx_lock().await;
		for (t, f) in tf {
			tfid.push((terms.resolve_term_id(&mut tx, t).await?, f));
		}
		drop(tx);
		Ok((dl, tfid))
	}

	/// This method is used for indexing.
	/// It will create new term ids for non already existing terms.
	pub(super) async fn extract_terms_with_frequencies_with_offsets(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		terms: &mut Terms,
		content: Vec<Value>,
	) -> Result<(DocLength, Vec<(TermId, TermFrequency)>, Vec<(TermId, OffsetRecords)>), Error> {
		let mut dl = 0;
		// Let's first collect all the inputs, and collect the tokens.
		// We need to store them because everything after is zero-copy
		let mut inputs = Vec::with_capacity(content.len());
		self.analyze_content(stk, ctx, opt, content, FilteringStage::Indexing, &mut inputs).await?;
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
		let mut tx = ctx.tx_lock().await;
		for (t, o) in tfos {
			let id = terms.resolve_term_id(&mut tx, t).await?;
			tfid.push((id, o.len() as TermFrequency));
			osid.push((id, OffsetRecords(o)));
		}
		drop(tx);
		Ok((dl, tfid, osid))
	}

	/// Was marked recursive
	async fn analyze_content(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		content: Vec<Value>,
		stage: FilteringStage,
		tks: &mut Vec<Tokens>,
	) -> Result<(), Error> {
		for v in content {
			self.analyze_value(stk, ctx, opt, v, stage, tks).await?;
		}
		Ok(())
	}

	/// Was marked recursive
	async fn analyze_value(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		val: Value,
		stage: FilteringStage,
		tks: &mut Vec<Tokens>,
	) -> Result<(), Error> {
		match val {
			Value::Strand(s) => tks.push(self.generate_tokens(stk, ctx, opt, stage, s).await?),
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

	async fn generate_tokens(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stage: FilteringStage,
		mut input: String,
	) -> Result<Tokens, Error> {
		if let Some(function_name) = self.function.clone() {
			let fns = Function::Custom(function_name.clone(), vec![Value::Strand(input)]);
			let val = fns.compute(stk, ctx, opt, None).await?;
			if let Value::Strand(val) = val {
				input = val;
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
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		input: String,
	) -> Result<Value, Error> {
		self.generate_tokens(stk, ctx, opt, FilteringStage::Indexing, input).await?.try_into()
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
		let ctx = Context::default().set_transaction(txn);

		let mut stmt = syn::parse(&format!("DEFINE {def}")).unwrap();
		let Some(Statement::Define(DefineStatement::Analyzer(az))) = stmt.0 .0.pop() else {
			panic!()
		};
		let a: Analyzer = az.into();

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
}
