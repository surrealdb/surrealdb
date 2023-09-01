use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::ft::docids::{DocId, DocIds};
use crate::idx::ft::scorer::BM25Scorer;
use crate::idx::ft::termdocs::TermsDocs;
use crate::idx::ft::terms::TermId;
use crate::idx::ft::{FtIndex, MatchRef};
use crate::idx::planner::iterators::{
	MatchesThingIterator, NonUniqueEqualThingIterator, ThingIterator, UniqueEqualThingIterator,
};
use crate::idx::planner::plan::IndexOption;
use crate::idx::planner::tree::IndexMap;
use crate::idx::trees::store::TreeStoreType;
use crate::idx::IndexKeyBase;
use crate::kvs;
use crate::kvs::Key;
use crate::sql::index::Index;
use crate::sql::{Expression, Operator, Table, Thing, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) type IteratorRef = u16;

pub(crate) struct QueryExecutor {
	table: String,
	ft_map: HashMap<String, FtIndex>,
	mr_entries: HashMap<MatchRef, FtEntry>,
	exp_entries: HashMap<Expression, FtEntry>,
	iterators: Vec<Expression>,
}

impl QueryExecutor {
	pub(super) async fn new(
		opt: &Options,
		txn: &Transaction,
		table: &Table,
		index_map: IndexMap,
	) -> Result<Self, Error> {
		let mut run = txn.lock().await;

		let mut mr_entries = HashMap::default();
		let mut exp_entries = HashMap::default();
		let mut ft_map = HashMap::default();

		// Create all the instances of FtIndex
		// Build the FtEntries and map them to Expressions and MatchRef
		for (exp, io) in index_map.consume() {
			let mut entry = None;
			if let Index::Search(p) = &io.ix().index {
				let ixn = &io.ix().name.0;
				if let Some(ft) = ft_map.get(ixn) {
					if entry.is_none() {
						entry = FtEntry::new(&mut run, ft, io).await?;
					}
				} else {
					let ikb = IndexKeyBase::new(opt, io.ix());
					let az = run.get_db_analyzer(opt.ns(), opt.db(), p.az.as_str()).await?;
					let ft = FtIndex::new(&mut run, az, ikb, p, TreeStoreType::Read).await?;
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

		Ok(Self {
			table: table.0.clone(),
			ft_map,
			mr_entries,
			exp_entries,
			iterators: Vec::new(),
		})
	}

	pub(super) fn add_iterator(&mut self, exp: Expression) -> IteratorRef {
		let ir = self.iterators.len();
		self.iterators.push(exp);
		ir as IteratorRef
	}

	pub(crate) fn is_distinct(&self, ir: IteratorRef) -> bool {
		(ir as usize) < self.iterators.len()
	}

	pub(crate) fn get_iterator_expression(&self, ir: IteratorRef) -> Option<&Expression> {
		self.iterators.get(ir as usize)
	}

	fn get_match_ref(match_ref: &Value) -> Option<MatchRef> {
		if let Value::Number(n) = match_ref {
			let m = n.to_int() as u8;
			Some(m)
		} else {
			None
		}
	}

	pub(crate) async fn new_iterator(
		&self,
		opt: &Options,
		ir: IteratorRef,
		io: IndexOption,
	) -> Result<Option<ThingIterator>, Error> {
		match &io.ix().index {
			Index::Idx => Self::new_index_iterator(opt, io),
			Index::Uniq => Self::new_unique_index_iterator(opt, io),
			Index::Search {
				..
			} => self.new_search_index_iterator(ir, io).await,
			_ => Err(Error::FeatureNotYetImplemented {
				feature: "VectorSearch iterator",
			}),
		}
	}

	fn new_index_iterator(opt: &Options, io: IndexOption) -> Result<Option<ThingIterator>, Error> {
		if io.op() == &Operator::Equal {
			return Ok(Some(ThingIterator::NonUniqueEqual(NonUniqueEqualThingIterator::new(
				opt,
				io.ix(),
				io.array(),
			)?)));
		}
		Ok(None)
	}

	fn new_unique_index_iterator(
		opt: &Options,
		io: IndexOption,
	) -> Result<Option<ThingIterator>, Error> {
		if io.op() == &Operator::Equal {
			return Ok(Some(ThingIterator::UniqueEqual(UniqueEqualThingIterator::new(
				opt,
				io.ix(),
				io.array(),
			)?)));
		}
		Ok(None)
	}

	async fn new_search_index_iterator(
		&self,
		ir: IteratorRef,
		io: IndexOption,
	) -> Result<Option<ThingIterator>, Error> {
		if let Some(exp) = self.iterators.get(ir as usize) {
			if let Operator::Matches(_) = io.op() {
				let ixn = &io.ix().name.0;
				if let Some(fti) = self.ft_map.get(ixn) {
					if let Some(fte) = self.exp_entries.get(exp) {
						let it = MatchesThingIterator::new(fti, fte.0.terms_docs.clone()).await?;
						return Ok(Some(ThingIterator::Matches(it)));
					}
				}
			}
		}
		Ok(None)
	}

	pub(crate) async fn matches(
		&self,
		txn: &Transaction,
		thg: &Thing,
		exp: &Expression,
	) -> Result<Value, Error> {
		// Otherwise, we look for the first possible index options, and evaluate the expression
		// Does the record id match this executor's table?
		if thg.tb.eq(&self.table) {
			if let Some(ft) = self.exp_entries.get(exp) {
				let mut run = txn.lock().await;
				let doc_key: Key = thg.into();
				if let Some(doc_id) =
					ft.0.doc_ids.read().await.get_doc_id(&mut run, doc_key).await?
				{
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
					doc_id = e.0.doc_ids.read().await.get_doc_id(&mut run, key).await?;
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
	doc_ids: Arc<RwLock<DocIds>>,
	terms: Vec<Option<TermId>>,
	terms_docs: TermsDocs,
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
				doc_ids: ft.doc_ids(),
				scorer: ft.new_scorer(terms_docs.clone())?,
				terms,
				terms_docs,
			}))))
		} else {
			Ok(None)
		}
	}
}
