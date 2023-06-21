use crate::dbs::{Options, Transaction};
use crate::err::Error;
§use crate::idx::ft::docids::{DocId, DocIds};
use crate::idx::ft::scorer::BM25Scorer;
use crate::idx::ft::terms::TermId;
use crate::idx::ft::{FtIndex, MatchRef};
use crate::idx::planner::plan::IndexOption;
use crate::idx::planner::tree::IndexMap;
use crate::idx::IndexKeyBase;
use crate::kvs;
use crate::kvs::Key;
use crate::sql::index::Index;
use crate::sql::{Expression, Operator, Table, Thing, Value};
use roaring::RoaringTreemap;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) struct QueryExecutor {
	table: String,
	pre_match_expression: Option<Expression>,
	pre_match_terms_docs: Option<Arc<Vec<(TermId, RoaringTreemap)>>>,
	ft_map: HashMap<String, FtIndex>,
	mr_entries: HashMap<MatchRef, MatchRefEntry>,
	index_options: HashMap<Expression, IndexOption>,
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

		let mut pre_match_terms_docs = None;
		let mut pre_match_ref = None;
		if let Some(pme) = &pre_match_expression {
			if let Operator::Matches(mr) = &pme.o {
				pre_match_ref = *mr;
			}
		}

		let mut index_options = HashMap::default();
		// Create all the instances of FtIndex
		let mut ft_map = HashMap::default();
		for (exp, ios) in index_map.0 {
			let mut exp_io = None;
			for io in ios {
				if let Index::Search {
					az,
					order,
					sc,
					hl,
				} = &io.ix().index
				{
					if exp_io.is_none() {
						exp_io = Some(io.clone());
					}
					let ixn = &io.ix().name.0;
					if !ft_map.contains_key(ixn) {
						let ikb = IndexKeyBase::new(opt, &io.ix());
						let az = run.get_az(opt.ns(), opt.db(), az.as_str()).await?;
						let ft = FtIndex::new(&mut run, az, ikb, *order, sc, *hl).await?;

						if pre_match_expression.as_ref() == Some(&exp) {
							if let Some(qs) = io.qs() {
								let term_ids = ft.extract_terms(&mut run, qs.to_owned()).await?;
								let td = Arc::new(ft.get_terms_docs(&mut run, &term_ids).await?);
								if let Some(mr) = &pre_match_ref {
									let mre = MatchRefEntry::new(
										&mut run,
										&ft,
										io.clone(),
										term_ids,
										td.clone(),
									)
									.await?;
									mr_entries.insert(*mr, mre);
								}
								pre_match_terms_docs = Some(td);
							}
						}
						ft_map.insert(ixn.to_owned(), ft);
					}
				}
			}
			if let Some(io) = exp_io {
				index_options.insert(exp, io);
			}
		}

		for (_, io) in &index_options {
			if let Some(ft) = ft_map.get(&io.ix().name.0) {
				if io.match_ref() == pre_match_ref.as_ref() {
					// We already have the MatchRefEntry
					continue;
				}
				if let Some(mr) = io.match_ref() {
					match mr_entries.entry(*mr) {
						Entry::Occupied(_) => {
							return Err(Error::DuplicatedMatchRef {
								mr: *mr,
							});
						}
						Entry::Vacant(e) => {
							if let Some(qs) = io.qs() {
								let term_ids = ft.extract_terms(&mut run, qs.to_owned()).await?;
								let td = Arc::new(ft.get_terms_docs(&mut run, &term_ids).await?);
								let mre = MatchRefEntry::new(
									&mut run,
									ft,
									io.clone(),
									term_ids,
									td.clone(),
								)
								.await?;
								e.insert(mre);
							}
						}
					}
				}
			}
		}
		Ok(Self {
			table: table.0.clone(),
			pre_match_expression,
			pre_match_terms_docs,
			ft_map,
			mr_entries,
			index_options,
		})
	}

	pub(super) fn pre_match_terms_docs(&self) -> Option<Arc<Vec<(TermId, RoaringTreemap)>>> {
		self.pre_match_terms_docs.clone()
	}

	fn get_match_ref(match_ref: &Value) -> Option<MatchRef> {
		if let Value::Number(n) = match_ref {
			let m = n.to_int() as u8;
			Some(m)
		} else {
			None
		}
	}

	pub(crate) async fn get_doc_id(
		&self,
		txn: &Transaction,
		match_ref: &Value,
		rid: &Thing,
		doc_id: Option<DocId>,
	) -> Result<Option<DocId>, Error> {
		if let Some(doc_id) = doc_id {
			return Ok(Some(doc_id));
		}
		if let Some(mr) = Self::get_match_ref(match_ref) {
			if let Some(e) = self.mr_entries.get(&mr) {
				let key: Key = rid.into();
				let mut run = txn.lock().await;
				return e.doc_ids.get_doc_id(&mut run, key).await;
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
		// Does the record id match this executor's table?
		if thg.tb.eq(&self.table) {
			if let Some(io) = self.index_options.get(exp) {
				if let Some(qs) = io.qs() {
					if let Some(fti) = self.ft_map.get(&io.ix().name.0) {
						// TODO The query string could be extracted when IndexOptions are created
						let mut run = txn.lock().await;
						return Ok(Value::Bool(
							fti.match_id_value(&mut run, thg, qs.as_str()).await?,
						));
					}
				} else {
					return Ok(Value::Bool(false));
				}
			}
		}

		// If no previous case were successful, we end up with a user error
		Err(Error::NoIndexFoundForMatch {
			value: exp.to_string(),
		})
	}

	fn get_match_ref_entry(&self, match_ref: &Value) -> Option<&MatchRefEntry> {
		if let Some(mr) = Self::get_match_ref(match_ref) {
			if let Some(e) = self.mr_entries.get(&mr) {
				return Some(e);
			}
		}
		None
	}

	fn get_match_ref_entry_and_fti(&self, match_ref: &Value) -> Option<(&MatchRefEntry, &FtIndex)> {
		if let Some(e) = self.get_match_ref_entry(match_ref) {
			if let Some(ft) = self.ft_map.get(&e.index_option.ix().name.0) {
				return Some((e, ft));
			}
		}
		None
	}

	pub(crate) async fn highlight(
		&self,
		txn: Transaction,
		thg: &Thing,
		prefix: Value,
		suffix: Value,
		match_ref: &Value,
		doc: &Value,
	) -> Result<Value, Error> {
		if let Some((e, ft)) = self.get_match_ref_entry_and_fti(match_ref) {
			let mut run = txn.lock().await;
			return ft
				.highlight(&mut run, thg, &e.terms, prefix, suffix, &e.index_option.id(), doc)
				.await;
		}
		Ok(Value::None)
	}

	pub(crate) async fn offsets(
		&self,
		txn: Transaction,
		thg: &Thing,
		match_ref: &Value,
	) -> Result<Value, Error> {
		if let Some((e, ft)) = self.get_match_ref_entry_and_fti(match_ref) {
			let mut run = txn.lock().await;
			return ft.extract_offsets(&mut run, thg, &e.terms).await;
		}
		Ok(Value::None)
	}

	pub(crate) async fn score(
		&self,
		txn: Transaction,
		match_ref: &Value,
		doc_id: DocId,
	) -> Result<Value, Error> {
		if let Some(e) = self.get_match_ref_entry(match_ref) {
			if let Some(scorer) = &e.scorer {
				let mut run = txn.lock().await;
				let score = scorer.score(&mut run, doc_id).await?;
				if let Some(score) = score {
					return Ok(Value::from(score));
				}
			}
		}
		Ok(Value::None)
	}
}

struct MatchRefEntry {
	index_option: IndexOption,
	doc_ids: DocIds,
	terms: Vec<TermId>,
	scorer: Option<BM25Scorer>,
}

impl MatchRefEntry {
	async fn new(
		tx: &mut kvs::Transaction,
		ft: &FtIndex,
		io: IndexOption,
		term_ids: Vec<TermId>,
		td: Arc<Vec<(TermId, RoaringTreemap)>>,
	) -> Result<Self, Error> {
		Ok(Self {
			index_option: io,
			doc_ids: ft.doc_ids(tx).await?,
			terms: term_ids,
			scorer: ft.new_scorer(tx, td).await?,
		})
	}
}
