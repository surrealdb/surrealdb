use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::docids::{DocId, DocIds};
use crate::idx::ft::scorer::BM25Scorer;
use crate::idx::ft::termdocs::TermsDocs;
use crate::idx::ft::terms::TermId;
use crate::idx::ft::{FtIndex, MatchRef};
use crate::idx::planner::executor::{IteratorRef, QueryExecutor};
use crate::idx::planner::iterators::{MatchesThingIterator, ThingIterator};
use crate::idx::planner::plan::{IndexOption, Lookup};
use crate::idx::trees::store::TreeStoreType;
use crate::idx::IndexKeyBase;
use crate::kvs;
use crate::kvs::Key;
use crate::sql::index::SearchParams;
use crate::sql::{Expression, Thing, Value};
use std::sync::Arc;
use tokio::sync::RwLock;

impl QueryExecutor {
	pub(super) async fn check_search_entry(
		&mut self,
		opt: &Options,
		run: &mut kvs::Transaction,
		io: &IndexOption,
		exp: Expression,
		p: &SearchParams,
	) -> Result<(), Error> {
		let ixn = &io.ix().name.0;
		let entry = if let Some(ft) = self.ft_map.get(ixn) {
			FtEntry::new(run, ft, io.clone()).await?
		} else {
			let ikb = IndexKeyBase::new(opt, io.ix());
			let az = run.get_db_analyzer(opt.ns(), opt.db(), p.az.as_str()).await?;
			let ft = FtIndex::new(run, az, ikb, p, TreeStoreType::Read).await?;
			let ixn = ixn.to_owned();
			let entry = FtEntry::new(run, &ft, io.clone()).await?;
			self.ft_map.insert(ixn, ft);
			entry
		};

		if let Some(e) = entry {
			if let Lookup::FtMatches {
				mr,
				..
			} = e.0.index_option.lo()
			{
				if let Some(mr) = mr {
					if self.ft_mr.insert(*mr, e.clone()).is_some() {
						return Err(Error::DuplicatedMatchRef {
							mr: *mr,
						});
					}
				}
				self.ft_exp.insert(exp, e);
			}
		}
		Ok(())
	}

	pub(super) async fn new_ft_index_matches_iterator(
		&self,
		ir: IteratorRef,
		io: IndexOption,
	) -> Result<Option<ThingIterator>, Error> {
		if let Some(exp) = self.iterators.get(ir as usize) {
			let ixn = &io.ix().name.0;
			if let Some(fti) = self.ft_map.get(ixn) {
				if let Some(fte) = self.ft_exp.get(exp) {
					let it = MatchesThingIterator::new(fti, fte.0.terms_docs.clone()).await?;
					return Ok(Some(ThingIterator::Matches(it)));
				}
			}
		}
		Ok(None)
	}

	pub(super) async fn new_mtree_index_knn_iterator(
		&self,
		_ir: IteratorRef,
		_io: IndexOption,
	) -> Result<Option<ThingIterator>, Error> {
		todo!()
	}

	pub(crate) async fn matches(
		&self,
		txn: &Transaction,
		thg: &Thing,
		exp: &Expression,
	) -> Result<Value, Error> {
		// Does the record id match this executor's table?
		if thg.tb.eq(&self.table) {
			if let Some(ft) = self.ft_exp.get(exp) {
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

	fn get_match_ref(match_ref: &Value) -> Option<MatchRef> {
		if let Value::Number(n) = match_ref {
			let m = n.to_int() as u8;
			Some(m)
		} else {
			None
		}
	}

	fn get_ft_entry(&self, match_ref: &Value) -> Option<&FtEntry> {
		if let Some(mr) = Self::get_match_ref(match_ref) {
			self.ft_mr.get(&mr)
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
pub(super) struct FtEntry(Arc<Inner>);

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
		if let Lookup::FtMatches {
			qs,
			..
		} = io.lo()
		{
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
