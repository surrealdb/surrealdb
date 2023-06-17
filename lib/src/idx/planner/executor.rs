use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::ft::docids::{DocId, DocIds};
use crate::idx::ft::scorer::BM25Scorer;
use crate::idx::ft::terms::TermId;
use crate::idx::ft::{FtIndex, MatchRef};
use crate::idx::planner::plan::IndexOption;
use crate::idx::planner::tree::IndexMap;
use crate::idx::IndexKeyBase;
use crate::kvs::Key;
use crate::sql::index::Index;
use crate::sql::{Expression, Idiom, Operator, Table, Thing, Value};
use roaring::RoaringTreemap;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub(crate) struct QueryExecutor {
	table: String,
	index: HashMap<Expression, HashSet<IndexOption>>,
	pre_match_expression: Option<Expression>,
	pre_match_terms_docs: Option<Arc<Vec<(TermId, RoaringTreemap)>>>,
	ft_map: HashMap<String, FtIndex>,
	doc_ids: HashMap<MatchRef, DocIds>,
	terms: HashMap<MatchRef, IndexFieldTerms>,
	scorers: HashMap<MatchRef, Option<BM25Scorer>>,
}

struct IndexFieldTerms {
	ix: String,
	id: Idiom,
	t: Vec<TermId>,
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
		let mut ft_map = HashMap::new();
		for ios in index_map.index.values() {
			for io in ios {
				if let Index::Search {
					az,
					order,
					sc,
					hl,
				} = &io.ix.index
				{
					if !ft_map.contains_key(&io.ix.name.0) {
						let ikb = IndexKeyBase::new(opt, &io.ix);
						let az = run.get_az(opt.ns(), opt.db(), az.as_str()).await?;
						let ft = FtIndex::new(&mut run, az, ikb, *order, sc, *hl).await?;
						ft_map.insert(io.ix.name.0.clone(), ft);
					}
				}
			}
		}
		let mut pre_match_ref = None;
		if let Some(e) = &pre_match_expression {
			if let Operator::Matches(mr) = e.o {
				pre_match_ref = mr;
			}
		}
		let mut pre_match_terms_docs = None;
		let mut terms = HashMap::with_capacity(index_map.terms.len());
		let mut scorers = HashMap::default();
		let mut doc_ids = HashMap::new();
		for (mr, ifv) in index_map.terms {
			if let Some(ft) = ft_map.get(&ifv.ix) {
				let term_ids = ft.extract_terms(&mut run, ifv.val.clone()).await?;
				match doc_ids.entry(mr) {
					Entry::Occupied(_) => {}
					Entry::Vacant(e) => {
						e.insert(ft.doc_ids(&mut run).await?);
					}
				}

				if let Some(pmr) = pre_match_ref {
					if pmr == mr {
						if let Some(td) = ft.get_terms_docs(&mut run, &term_ids).await? {
							pre_match_terms_docs = Some(Arc::new(td));
						}
					}
				}

				// Check if we have a scorer
				match scorers.entry(mr) {
					Entry::Occupied(_) => {}
					Entry::Vacant(e) => {
						let mut td = None;
						if let Some(pmr) = pre_match_ref {
							if pmr == mr {
								if let Some(pmtd) = &pre_match_terms_docs {
									td = Some(pmtd.clone());
								}
							}
						}
						if td.is_none() {
							if let Some(t) = ft.get_terms_docs(&mut run, &term_ids).await? {
								td = Some(Arc::new(t));
							}
						}

						if let Some(td) = td {
							e.insert(ft.new_scorer(&mut run, td.clone()).await?);
						} else {
							e.insert(None);
						}
					}
				}
				terms.insert(
					mr,
					IndexFieldTerms {
						ix: ifv.ix,
						id: ifv.id,
						t: term_ids,
					},
				);
			}
		}
		Ok(Self {
			table: table.0.clone(),
			index: index_map.index,
			scorers,
			pre_match_expression,
			pre_match_terms_docs,
			ft_map,
			terms,
			doc_ids,
		})
	}

	pub(super) fn pre_match_terms_docs(&self) -> Option<Arc<Vec<(TermId, RoaringTreemap)>>> {
		self.pre_match_terms_docs.clone()
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
		if let Value::Number(n) = match_ref {
			let m = n.to_int() as u8;
			if let Some(doc_ids) = self.doc_ids.get(&m) {
				let key: Key = rid.into();
				let mut run = txn.lock().await;
				return doc_ids.get_doc_id(&mut run, key).await;
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
			if let Some(ios) = self.index.get(exp) {
				for io in ios {
					if let Some(fti) = self.ft_map.get(&io.ix.name.0) {
						// TODO The query string could be extracted when IndexOptions are created
						let query_string = io.v.clone().convert_to_string()?;
						let mut run = txn.lock().await;
						return Ok(Value::Bool(
							fti.match_id_value(&mut run, thg, &query_string).await?,
						));
					}
				}
			}
		}

		// If no previous case were successful, we end up with a user error
		Err(Error::NoIndexFoundForMatch {
			value: exp.to_string(),
		})
	}

	async fn get_ft_index(
		&self,
		match_ref: &Value,
	) -> Result<Option<(&IndexFieldTerms, &FtIndex)>, Error> {
		// We have to make the connection between the match ref from the highlight function...
		if let Value::Number(n) = match_ref {
			let m = n.to_int() as u8;
			// ... and from the match operator (@{matchref}@)
			if let Some(ift) = self.terms.get(&m) {
				// Check we have an index?
				if let Some(ft) = self.ft_map.get(&ift.ix) {
					return Ok(Some((ift, ft)));
				}
			}
		}
		Ok(None)
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
		if let Some((ift, ft)) = self.get_ft_index(match_ref).await? {
			let mut run = txn.lock().await;
			return ft.highlight(&mut run, thg, &ift.t, prefix, suffix, &ift.id, doc).await;
		}
		Ok(Value::None)
	}

	pub(crate) async fn offsets(
		&self,
		txn: Transaction,
		thg: &Thing,
		match_ref: &Value,
	) -> Result<Value, Error> {
		if let Some((ift, ft)) = self.get_ft_index(match_ref).await? {
			let mut run = txn.lock().await;
			return ft.extract_offsets(&mut run, thg, &ift.t).await;
		}
		Ok(Value::None)
	}

	pub(crate) async fn score(
		&self,
		txn: Transaction,
		match_ref: &Value,
		doc_id: DocId,
	) -> Result<Value, Error> {
		if let Value::Number(n) = match_ref {
			let m = n.to_int() as u8;
			if let Some(scorer) = self.scorers.get(&m) {
				if let Some(scorer) = scorer {
					let mut run = txn.lock().await;
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
