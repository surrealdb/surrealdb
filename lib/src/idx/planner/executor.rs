use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::ft::docids::{DocId, DocIds};
use crate::idx::ft::scorer::BM25Scorer;
use crate::idx::ft::termdocs::TermsDocs;
use crate::idx::ft::terms::TermId;
use crate::idx::ft::{FtIndex, MatchRef};
use crate::idx::planner::plan::IndexOption;
use crate::idx::planner::tree::IndexMap;
use crate::idx::IndexKeyBase;
use crate::kvs;
use crate::kvs::Key;
use crate::sql::index::Index;
use crate::sql::{Expression, Table, Thing, Value};
use roaring::RoaringTreemap;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) struct QueryExecutor {
	table: String,
	pre_match_expression: Option<Expression>,
	pre_match_entry: Option<FtEntry>,
	ft_map: HashMap<String, FtIndex>,
	mr_entries: HashMap<MatchRef, FtEntry>,
	exp_entries: HashMap<Expression, FtEntry>,
}

impl QueryExecutor {
	pub(super) async fn new(
		opt: &Options,
		txn: &Transaction,
		table: &Table,
		index_map: IndexMap,
		pre_match_expression: Option<Expression>,
	) -> Result<Self, Error> {
		let mut run = txn.lock().await;

		let mut mr_entries = HashMap::default();
		let mut exp_entries = HashMap::default();
		let mut ft_map = HashMap::default();

		// Create all the instances of FtIndex
		// Build the FtEntries and map them to Expressions and MatchRef
		for (exp, io) in index_map.consume() {
			let mut entry = None;
			if let Index::Search {
				az,
				order,
				sc,
				hl,
			} = &io.ix().index
			{
				let ixn = &io.ix().name.0;
				if let Some(ft) = ft_map.get(ixn) {
					if entry.is_none() {
						entry = FtEntry::new(&mut run, ft, io).await?;
					}
				} else {
					let ikb = IndexKeyBase::new(opt, io.ix());
					let az = run.get_az(opt.ns(), opt.db(), az.as_str()).await?;
					let ft = FtIndex::new(&mut run, az, ikb, *order, sc, *hl).await?;
					let ixn = ixn.to_owned();
					if entry.is_none() {
						entry = FtEntry::new(&mut run, &ft, io).await?;
					}
					ft_map.insert(ixn, ft);
				}
			}

			if let Some(e) = entry {
				if let Some(mr) = e.0.index_option.match_ref() {
					if mr_entries.insert(*mr, e.clone()).is_some() {
						return Err(Error::DuplicatedMatchRef {
							mr: *mr,
						});
					}
				}
				exp_entries.insert(exp, e);
			}
		}

		let mut pre_match_entry = None;
		if let Some(exp) = &pre_match_expression {
			pre_match_entry = exp_entries.get(exp).cloned();
		}
		Ok(Self {
			table: table.0.clone(),
			pre_match_expression,
			pre_match_entry,
			ft_map,
			mr_entries,
			exp_entries,
		})
	}

	pub(super) fn pre_match_terms_docs(&self) -> Option<TermsDocs> {
		if let Some(entry) = &self.pre_match_entry {
			return Some(entry.0.terms_docs.clone());
		}
		None
	}

	fn get_match_ref(match_ref: &Value) -> Option<MatchRef> {
		if let Value::Number(n) = match_ref {
			let m = n.to_int() as u8;
			Some(m)
		} else {
			None
		}
	}

	pub(crate) async fn matches(
		&self,
		txn: &Transaction,
		thg: &Thing,
		exp: &Expression,
	) -> Result<Value, Error> {
		// If we find the expression in `pre_match_expression`,
		// it means that we are using an Iterator::Index
		// and we are iterating over document that already matches the expression.
		if let Some(pme) = &self.pre_match_expression {
			if pme.eq(exp) {
				return Ok(Value::Bool(true));
			}
		}

		// Otherwise, we look for the first possible index options, and evaluate the expression
		// Does the record id match this executor's table?
		if thg.tb.eq(&self.table) {
			if let Some(ft) = self.exp_entries.get(exp) {
				let mut run = txn.lock().await;
				let doc_key: Key = thg.into();
				if let Some(doc_id) = ft.0.doc_ids.get_doc_id(&mut run, doc_key).await? {
					let term_goals = ft.0.terms_docs.len();
					// If there is no terms, it can't be a match
					if term_goals == 0 {
						return Ok(Value::Bool(false));
					}
					for opt_td in ft.0.terms_docs.iter() {
						if let Some((_, docs)) = opt_td {
							if !docs.contains(doc_id) {
								return Ok(Value::Bool(false));
							}
						} else {
							// If one of the term is missing, it can't be a match
							return Ok(Value::Bool(false));
						}
					}
					return Ok(Value::Bool(true));
				}
				return Ok(Value::Bool(false));
			}
		}

		// If no previous case were successful, we end up with a user error
		Err(Error::NoIndexFoundForMatch {
			value: exp.to_string(),
		})
	}

	fn get_ft_entry(&self, match_ref: &Value) -> Option<&FtEntry> {
		if let Some(mr) = Self::get_match_ref(match_ref) {
			self.mr_entries.get(&mr)
		} else {
			None
		}
	}

	fn get_ft_entry_and_index(&self, match_ref: &Value) -> Option<(&FtEntry, &FtIndex)> {
		if let Some(e) = self.get_ft_entry(match_ref) {
			if let Some(ft) = self.ft_map.get(&e.0.index_option.ix().name.0) {
				return Some((e, ft));
			}
		}
		None
	}

	pub(crate) async fn highlight(
		&self,
		txn: &Transaction,
		thg: &Thing,
		prefix: Value,
		suffix: Value,
		match_ref: &Value,
		doc: &Value,
	) -> Result<Value, Error> {
		if let Some((e, ft)) = self.get_ft_entry_and_index(match_ref) {
			let mut run = txn.lock().await;
			return ft
				.highlight(&mut run, thg, &e.0.terms, prefix, suffix, e.0.index_option.id(), doc)
				.await;
		}
		Ok(Value::None)
	}

	pub(crate) async fn offsets(
		&self,
		txn: &Transaction,
		thg: &Thing,
		match_ref: &Value,
	) -> Result<Value, Error> {
		if let Some((e, ft)) = self.get_ft_entry_and_index(match_ref) {
			let mut run = txn.lock().await;
			return ft.extract_offsets(&mut run, thg, &e.0.terms).await;
		}
		Ok(Value::None)
	}

	pub(crate) async fn score(
		&self,
		txn: &Transaction,
		match_ref: &Value,
		rid: &Thing,
		mut doc_id: Option<DocId>,
	) -> Result<Value, Error> {
		if let Some(e) = self.get_ft_entry(match_ref) {
			if let Some(scorer) = &e.0.scorer {
				let mut run = txn.lock().await;
				if doc_id.is_none() {
					let key: Key = rid.into();
					doc_id = e.0.doc_ids.get_doc_id(&mut run, key).await?;
				};
				if let Some(doc_id) = doc_id {
					let score = scorer.score(&mut run, doc_id).await?;
					if let Some(score) = score {
						return Ok(Value::from(score));
					}
				}
			}
		}
		Ok(Value::None)
	}
}

#[derive(Clone)]
struct FtEntry(Arc<Inner>);

struct Inner {
	index_option: IndexOption,
	doc_ids: DocIds,
	terms: Vec<Option<TermId>>,
	terms_docs: Arc<Vec<Option<(TermId, RoaringTreemap)>>>,
	scorer: Option<BM25Scorer>,
}

impl FtEntry {
	async fn new(
		tx: &mut kvs::Transaction,
		ft: &FtIndex,
		io: IndexOption,
	) -> Result<Option<Self>, Error> {
		if let Some(qs) = io.qs() {
			let terms = ft.extract_terms(tx, qs.to_owned()).await?;
			let terms_docs = Arc::new(ft.get_terms_docs(tx, &terms).await?);
			Ok(Some(Self(Arc::new(Inner {
				index_option: io,
				doc_ids: ft.doc_ids(tx).await?,
				scorer: ft.new_scorer(tx, terms_docs.clone()).await?,
				terms,
				terms_docs,
			}))))
		} else {
			Ok(None)
		}
	}
}
